using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

// =====================================================================================
// 模块：CacheStore / TableStore —— 缓存实例的内部仓库（全部 internal，业务不直接接触）
//
// 职责：
//   1. TableStore：单张表的存储单元 = 数据字典（UID → 对象）+ 去重脏队列；
//   2. CacheStore：一个 Client 的全部表 + Manager 类型注册表 + 字段变更路由。
//
// 相对旧版 MCache（CacheData）的关键修复：
//   1. 脏数据用「ConcurrentQueue + 去重 Set」精确追踪，落库线程不再全表扫描找 IsDirty
//      （旧版 O(表数×脏数据量) 的 ContainsUID/Get 全表线性遍历彻底消失）；
//   2. 按 UID 取对象直接命中本表字典，不再跨全部表线性遍历；
//   3. 变更路由按表名 O(1) 直达 Manager，替代旧版「静态事件 + 全 Manager 广播 +
//      每个接收方自己过滤表名」的 O(#Manager) 模式；
//   4. 实例级（非静态）：多套缓存（不同 CacheName）互不干扰，可单测隔离。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// 单张缓存表的存储单元：数据字典 + 去重脏队列。
    /// </summary>
    internal sealed class TableStore
    {
        /// <summary>表名（= 实体类型名，同时是 Redis Key 的最后一段）</summary>
        public readonly string TableName;

        /// <summary>本表全部对象：UID → CacheObject（含 IsDeleted 隐藏态对象，查询侧自行过滤）</summary>
        public readonly ConcurrentDictionary<long, CacheObject> Data = new ConcurrentDictionary<long, CacheObject>();

        // 脏队列：队列保证处理顺序，去重集合保证同一 UID 不会重复入队。
        // MarkDirty 幂等：落库失败重新 MarkDirty 即可安全重试。
        private readonly ConcurrentQueue<long> _dirtyQueue = new ConcurrentQueue<long>();
        private readonly ConcurrentDictionary<long, byte> _dirtySet = new ConcurrentDictionary<long, byte>();

        private int _managerBound;   // 装配守卫（0=未装配，1=已装配），Interlocked 原子翻转

        /// <summary>本表绑定的 Manager（volatile 保证跨线程可见；装配期由构造函数写入）</summary>
        public volatile ICacheManager Manager;

        /// <summary>是否启用过期淘汰（来自 [CacheBehavior] 特性）</summary>
        public bool EvictExpired { get; set; }

        /// <summary>过期时间（秒，来自 [CacheBehavior] 特性）</summary>
        public int ExpiredSeconds { get; set; }

        /// <param name="tableName">表名（实体类型名）</param>
        public TableStore(string tableName)
        {
            TableName = tableName;
        }

        /// <summary>
        /// 保证同一张表只有一个 Manager 完成装配。
        /// 关键修复：旧版守卫是 Manager 的实例字段，每次 new 都重复初始化 +
        /// 向静态事件重复挂接处理器（内存泄漏 + 变更重复处理）；
        /// 这里用 Interlocked 原子翻转，且 CacheStore.GetTable 保证同表同实例。
        /// </summary>
        /// <returns>true = 抢到装配权（首次）；false = 已有 Manager</returns>
        public bool BindManager() => Interlocked.Exchange(ref _managerBound, 1) == 0;

        /// <summary>
        /// 标记对象为脏（幂等）：未在队列中才入队。
        /// 调用方：Add / 字段变更 / 删除 / 落库失败重试。
        /// </summary>
        /// <param name="uid">对象 UID（&lt;=0 忽略）</param>
        public void MarkDirty(long uid)
        {
            if (uid <= 0) return;
            if (_dirtySet.TryAdd(uid, 0))
                _dirtyQueue.Enqueue(uid);
        }

        /// <summary>
        /// 取走至多 max 条脏 UID（落库线程调用）。
        /// 出队即移出去重集合 —— 落库失败时由 Manager 重新 MarkDirty 回队（安全重试）。
        /// </summary>
        /// <param name="max">最大取走数量</param>
        /// <returns>本轮取走的 UID 列表（可能少于 max）</returns>
        public List<long> TakeDirty(int max)
        {
            var result = new List<long>(Math.Min(max, 64));
            while (result.Count < max && _dirtyQueue.TryDequeue(out long uid))
            {
                _dirtySet.TryRemove(uid, out _);
                result.Add(uid);
            }
            return result;
        }
    }

    /// <summary>
    /// 一个 MRedisCacheClient 实例的内部仓库：表存储 + Manager 注册表 + 变更路由。
    /// 实例级（非静态），多套缓存互不干扰。
    /// </summary>
    internal sealed class CacheStore
    {
        /// <summary>缓存名（Redis Key 前缀 / 发布频道后缀）</summary>
        public readonly string CacheName;

        /// <summary>表名 → TableStore（同表同实例，ConcurrentDictionary 保证）</summary>
        private readonly ConcurrentDictionary<string, TableStore> _tables = new ConcurrentDictionary<string, TableStore>();

        /// <summary>实体类型 → Manager（类型注册表，GetManager 与路由的查询源）</summary>
        private readonly ConcurrentDictionary<Type, ICacheManager> _managers = new ConcurrentDictionary<Type, ICacheManager>();

        /// <param name="cacheName">缓存名</param>
        public CacheStore(string cacheName)
        {
            CacheName = cacheName;
        }

        /// <summary>全部表存储（落库线程 / 对账遍历用）</summary>
        public IEnumerable<TableStore> Tables => _tables.Values;

        /// <summary>
        /// 取（或创建）指定表名的存储单元。
        /// 注意：GetTable 只保证存储唯一，不代表已有 Manager 装配
        /// （远端变更先于本地 Manager 创建到达时会出现"有表无 Manager"）。
        /// </summary>
        public TableStore GetTable(string tableName)
        {
            return _tables.GetOrAdd(tableName, n => new TableStore(n));
        }

        /// <summary>
        /// 按表名查找已装配的 Manager（远端变更路由用，O(1)）。
        /// 找不到返回 null（本实例未缓存该类型，调用方安全忽略）。
        /// </summary>
        public ICacheManager FindManager(string tableName)
        {
            return _tables.TryGetValue(tableName, out TableStore table) ? table.Manager : null;
        }

        /// <summary>
        /// 获取（或创建）实体类型对应的 Manager。
        ///
        /// 创建路径：直接 new（CacheManager 构造函数内部完成自注册到本注册表）；
        /// 并发竞争失败方捕获 InvalidOperationException 后重查注册表返回先注册者。
        ///
        /// 支持业务子类：先手动 new 子类 Manager（如 XxxMgr），其构造函数注册自身，
        /// 之后 GetManager&lt;T&gt;() 通过 TryGetValue 命中并返回同一实例（带自定义索引）。
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="client">所属客户端</param>
        public CacheManager<T> ResolveManager<T>(MRedisCacheClient client) where T : CacheObject, new()
        {
            if (_managers.TryGetValue(typeof(T), out ICacheManager existing))
                return (CacheManager<T>)existing;
            try
            {
                return new CacheManager<T>(client);
            }
            catch (InvalidOperationException)
            {
                // 并发竞争：另一个线程已抢先装配完成，重查注册表取既有实例
                return (CacheManager<T>)_managers[typeof(T)];
            }
        }

        /// <summary>注册 Manager（由 CacheManager 构造函数调用，键 = 实体类型）</summary>
        internal void RegisterManager(Type entityType, ICacheManager manager)
        {
            _managers[entityType] = manager;
        }

        /// <summary>
        /// 字段变更路由（CacheObject.SetProperty 调用）：
        /// 按表名找到 Manager 转发 —— O(1) 精确路由，无广播。
        /// 找不到 Manager（对象尚未入缓存 / 类型未装配）时告警并丢弃。
        /// </summary>
        /// <param name="obj">变更的对象</param>
        /// <param name="fieldName">变化的属性名</param>
        /// <param name="oldValue">变更前的值</param>
        internal void NotifyFieldChanged(CacheObject obj, string fieldName, object oldValue)
        {
            TableStore table = GetTable(obj.TableName());
            ICacheManager manager = table.Manager;
            if (manager == null)
            {
                Log.Warn($"对象 {obj.TableName()}:{obj.CacheObjectUID} 发生变更但没有绑定 Manager，变更被忽略。");
                return;
            }
            manager.OnLocalFieldChanged(obj, fieldName, oldValue);
        }
    }
}
