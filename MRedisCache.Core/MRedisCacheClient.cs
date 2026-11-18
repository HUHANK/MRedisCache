using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

// =====================================================================================
// 模块：MRedisCacheClient —— 缓存实例入口（业务侧的装配与生命周期管理者）
//
// 职责：
//   1. 装配：按 Options 创建 PersistenceService（SqlSugar 落库）与 RedisSyncService（同步）；
//   2. 工厂：GetManager&lt;T&gt;() / GetMap&lt;V&gt;(name) 创建并缓存管理器与字典；
//   3. 生命周期：CompleteInitialLoad（切换到实时应用）与 Dispose（最后一轮落库 + 超时关闭）；
//   4. 一致性兜底：周期对账定时器 + 连接恢复触发的延迟对账。
//
// 典型生命周期（对应架构文档 3.1 节）：
//   using (var client = MRedisCacheClient.Create(options))
//   {
//       var mgr = new StationMgr(client);        // 或 client.GetManager&lt;Station&gt;()
//       client.CompleteInitialLoad();            // 全部 Manager 创建完成后调用
//       ... 业务读写 ...
//   }                                           // Dispose 时最后一轮落库
//
// 关键设计（相对旧版 MCache 静态类的修复）：
//   - 实例级：一个进程可创建多套缓存（不同 CacheName 互不干扰），单测可隔离；
//   - InstanceUid：实例唯一识别码，用于 Pub/Sub 防回声；
//   - Dispose 全链路带超时（10 秒），杜绝旧版 terminate 挂死。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// MRedisCache 客户端：一套缓存实例的入口（实例级，非全局静态，可多实例共存）。
    /// </summary>
    public sealed class MRedisCacheClient : IDisposable
    {
        /// <summary>缓存名（Redis Key 前缀 / 发布频道后缀），同一 Redis 中不同缓存必须不同名</summary>
        public string CacheName { get; }

        /// <summary>实例唯一识别码（63 位，发布消息的 Sender 字段，订阅端据此防回声）</summary>
        public long InstanceUid { get; }

        // ---------------- 内部服务句柄 ----------------

        /// <summary>缓存配置（只读快照）</summary>
        internal MRedisCacheOptions Options { get; }

        /// <summary>内部仓库：表存储 + Manager 注册表 + 变更路由</summary>
        internal CacheStore Store { get; }

        /// <summary>Redis 同步服务（未配置 Redis 时为 null）</summary>
        internal RedisSyncService Redis { get; private set; }

        /// <summary>数据库持久化服务（未启用数据库时为 null）</summary>
        internal PersistenceService Persistence { get; private set; }

        /// <summary>CacheMap 注册表（Redis Key → 路由信息，远端变更分发用）</summary>
        internal CacheMapRegistry Maps { get; } = new CacheMapRegistry();

        // ---------------- 状态 ----------------

        /// <summary>Map 名称 → Map 实例（类型擦除缓存；GetMap 的幂等来源）</summary>
        private readonly ConcurrentDictionary<string, object> _mapCache = new ConcurrentDictionary<string, object>();

        /// <summary>初始加载是否完成（volatile 保证订阅线程即时可见；未完成时远端变更被缓冲）</summary>
        private volatile bool _initialLoadCompleted;

        private int _disposed;                    // Dispose 幂等守卫
        private Timer _reconcileTimer;            // 周期对账定时器（ReconcileIntervalSeconds）
        private Timer _drainTimer;                // 2 秒兜底 drain 定时器（防缓冲消息滞留）

        /// <summary>初始加载是否完成（未完成时收到的远端变更会被缓冲）</summary>
        public bool InitialLoadCompleted => _initialLoadCompleted;

        /// <summary>私有构造（统一入口 Create，保证服务装配顺序）</summary>
        private MRedisCacheClient(MRedisCacheOptions options)
        {
            Options = options;
            CacheName = string.IsNullOrEmpty(options.CacheName) ? "MRedisCache" : options.CacheName;
            InstanceUid = Internal.UniqueIdGenerator.Next();
            Store = new CacheStore(CacheName);
        }

        /// <summary>
        /// 创建缓存实例，按配置装配数据库持久化与 Redis 同步。
        /// 装配顺序（对应架构文档 3.1 节第 1 步）：
        ///   1. 校验配置（启用数据库必须有连接串）；
        ///   2. EnableDatabase → 创建 PersistenceService（落库线程随之启动）；
        ///   3. RedisEndpoints 非空 → 创建 RedisSyncService（写线程/应用线程/订阅随之启动）
        ///      + 周期对账定时器 + 2 秒兜底 drain 定时器。
        /// </summary>
        /// <param name="options">缓存配置</param>
        /// <exception cref="ArgumentNullException">options 为空</exception>
        /// <exception cref="ArgumentException">启用数据库但未提供连接串</exception>
        public static MRedisCacheClient Create(MRedisCacheOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            var client = new MRedisCacheClient(options);

            // ① 数据库持久化（write-behind 落库线程）
            if (options.EnableDatabase)
            {
                if (string.IsNullOrEmpty(options.ConnectionString))
                    throw new ArgumentException("启用数据库（EnableDatabase=true）时必须提供 ConnectionString。");
                client.Persistence = new PersistenceService(client, options);
            }

            // ② Redis 同步（写线程 + 应用线程 + 订阅）
            if (options.RedisEndpoints != null && options.RedisEndpoints.Count > 0)
            {
                client.Redis = new RedisSyncService(client, options);

                // 周期对账：弥补 Pub/Sub 丢消息（可选，0 = 关闭）。
                // 语义：全表 HGET_ALL 按 CacheUpdateTime 合并 + 全部 Map 全量重载，
                // 与连接恢复触发的对账共用同一段逻辑（SafeReconcile）。
                if (options.ReconcileIntervalSeconds > 0)
                {
                    int dueMs = options.ReconcileIntervalSeconds * 1000;
                    client._reconcileTimer = new Timer(_ => SafeReconcile(client), null, dueMs, dueMs);
                }

                // 兜底定时器：防 CompleteInitialLoad 与订阅消息投递之间的竞态导致缓冲消息滞留。
                // 竞态窗口：订阅线程读到 InitialLoadCompleted=false 后被暂停，主线程恰好置位并
                // drain（此刻队列还是空的），订阅线程恢复后把消息放进 _pendingDuringLoad →
                // 最坏滞留 2 秒被本定时器救回（不会永久丢失）。
                client._drainTimer = new Timer(_ =>
                {
                    if (client._initialLoadCompleted) client.Redis.DrainPendingDuringLoad();
                }, null, 2000, 2000);
            }

            Log.Info($"MRedisCache[{client.CacheName}] 创建完成，实例 UID = {client.InstanceUid}。");
            return client;
        }

        /// <summary>
        /// 获取（或创建）实体类型的 Manager。
        /// 带自定义索引的 Manager 请先手动 new 子类，之后本方法返回同一实例；
        /// 同一实体类型全进程只装配一次（重复创建抛异常，由 Store.ResolveManager 兜底转换）。
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        public CacheManager<T> GetManager<T>() where T : CacheObject, new()
        {
            ThrowIfDisposed();
            return Store.ResolveManager<T>(this);
        }

        /// <summary>
        /// 获取（或创建）Redis 同步的字符串字典。
        /// 幂等：同名重复调用返回同一实例；同名不同 V 类型抛异常（防止路由表错乱）。
        /// </summary>
        /// <typeparam name="V">值类型（必须为引用类型）</typeparam>
        /// <param name="name">Map 名称（同一 Client 内唯一）</param>
        public CacheMap<V> GetMap<V>(string name) where V : class
        {
            ThrowIfDisposed();
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("Map 名称不能为空。", nameof(name));

            object existing = _mapCache.GetOrAdd(name, n => CreateMap<V>(n));
            CacheMap<V> map = existing as CacheMap<V>;
            if (map == null)
                throw new InvalidOperationException($"Map[{name}] 已以其他值类型注册，同一名称不允许更换 V 类型。");
            return map;
        }

        /// <summary>
        /// 创建 Map 并注册远端路由信息（首次 GetMap 时调用一次）。
        /// 创建即从 Redis 全量加载（CacheMap 构造函数内）。
        /// </summary>
        private CacheMap<V> CreateMap<V>(string name) where V : class
        {
            var map = new CacheMap<V>(this, name);
            Maps.Register(map.RedisKey, new CacheMapInfo
            {
                Upsert = map.OnRemoteUpsert,
                Delete = map.OnRemoteDelete,
                Reload = map.ReloadFromRedis
            });
            return map;
        }

        /// <summary>
        /// 全部 Manager 创建完成后调用：此后远端变更通知开始实时应用（此前为缓冲）。
        /// 语义：置位 InitialLoadCompleted（volatile，订阅线程立即可见）
        ///       → DrainPendingDuringLoad 把缓冲消息转入应用队列。
        /// Redis 未启用时调用无副作用。
        /// </summary>
        public void CompleteInitialLoad()
        {
            _initialLoadCompleted = true;
            Redis?.DrainPendingDuringLoad();
            Log.Info("初始加载完成，远端变更通知开始实时应用。");
        }

        /// <summary>
        /// Redis 连接恢复等场景触发的延迟对账（5 秒后执行，避开重连抖动期）。
        /// 一次性定时器：触发即自释放。
        /// </summary>
        internal void ScheduleReconciliation()
        {
            if (Redis == null || _disposed == 1) return;
            Timer timer = null;
            timer = new Timer(_ =>
            {
                timer?.Dispose();
                SafeReconcile(this);
            }, null, 5000, Timeout.Infinite);
        }

        /// <summary>
        /// 对账的统一执行体（静态方法避免闭包持有 Timer）：
        ///   1. 全部 Manager.LoadFromRedis —— HGET_ALL 后按 CacheUpdateTime 合并（旧不盖新）；
        ///   2. 全部 CacheMap.ReloadFromRedis —— 清空本地后全量重载。
        /// 任一步骤异常只记日志，不影响其他表。
        /// </summary>
        private static void SafeReconcile(MRedisCacheClient client)
        {
            try
            {
                if (client._disposed == 1 || client.Redis == null) return;
                foreach (TableStore table in client.Store.Tables)
                {
                    ICacheManager manager = table.Manager;
                    if (manager != null) manager.LoadFromRedis();
                }
                foreach (CacheMapInfo map in client.Maps.All())
                    map.Reload?.Invoke();
                Log.Debug("Redis 对账合并完成。");
            }
            catch (Exception ex)
            {
                Log.Error("Redis 对账失败", ex);
            }
        }

        /// <summary>
        /// 关闭缓存实例（幂等）：
        ///   1. 停对账/兜底定时器；
        ///   2. 先停 PersistenceService（含最后一轮落库，尽量不丢数据）；
        ///   3. 再关 RedisSyncService（各后台线程 10 秒超时强关）。
        /// 顺序理由：落库要用 Redis 之外的数据库连接，先完成最后一轮落库再断同步。
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            _reconcileTimer?.Dispose();
            _drainTimer?.Dispose();
            // 先停落库（含最后一轮落库），再关 Redis
            Persistence?.Dispose();
            Redis?.Dispose();
            Log.Info($"MRedisCache[{CacheName}] 已关闭。");
        }

        /// <summary>已关闭后访问 API 的统一防线</summary>
        private void ThrowIfDisposed()
        {
            if (_disposed == 1) throw new ObjectDisposedException(nameof(MRedisCacheClient));
        }
    }
}
