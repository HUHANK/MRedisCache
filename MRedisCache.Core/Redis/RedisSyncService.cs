using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using MRedisCache.Internal;
using StackExchange.Redis;

// =====================================================================================
// 模块：RedisSyncService —— Redis 同步服务（L2 层核心）
//
// 职责：
//   1. 写路径（本实例 → Redis）：接收 Manager 投递的对象变更，经无界 Channel 异步批量
//      写入 Redis Hash，并通过 Lua 脚本原子地完成「写入 + 版本号递增 + 发布通知」；
//   2. 订阅路径（Redis → 本实例）：订阅变更频道，收到其他实例的通知后按 (Key, Field)
//      去重、按 Key 合并 HMGET 批量回读最新值，再合并进本地内存；
//   3. 可靠性：写入失败无限重试（3 秒间隔）；连接恢复后触发全表对账合并；
//   4. 读接口：为 Manager 提供单字段 / 全 Hash 的读取能力（启动加载与读穿透使用）。
//
// 线程模型（共 3 类参与者）：
//   - 业务线程：调用 EnqueueXxx 投递写入（TryWrite，微秒级返回，永不阻塞）；
//   - 写线程（_worker）：单消费者批量执行 Lua 写入，失败重试；
//   - 应用线程（_applyWorker）：单消费者批量回读 + 应用远端变更；
//   - StackExchange.Redis 订阅泵：回调 OnSubscriptionMessage，只做解析 + 投递。
//
// 数据形态约定（全系统唯一）：
//   - Redis Key（实体表）：{CacheName}.{TypeName}，Hash field = 对象 UID，value = 整对象 JSON；
//   - Redis Key（Map）：{CacheName}.Map.{MapName}，Hash field = 字典 key，value = 值 JSON；
//   - 每个Hash 额外维护一个 "VERSION" 字段（HINCRBY 递增），仅作变更计数观测用；
//   - 发布频道：MRedisCache.DataChanged.{CacheName}，消息只带定位符（Key+Field）不带数据。
//
// 相对旧版 MCache 的关键修复：
//   - 旧版字段级 HUPT 与远端整体 JSON 反序列化协议不对称 → 统一为整对象 + 回读模式；
//   - 旧版忙等线程（0.1ms 自旋）烧 CPU 且 terminate 可能挂死 → Channel + async + 超时关闭；
//   - 旧版 Pub/Sub 丢消息无补偿 → 连接恢复对账 + Client 侧周期对账双兜底。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>Redis 写操作类型</summary>
    internal enum RedisOp
    {
        /// <summary>写入/覆盖（HSET 整对象 JSON）</summary>
        Upsert,

        /// <summary>删除（HDEL 单 field）</summary>
        Delete
    }

    /// <summary>Redis 写请求（业务线程 → 写线程的传递单元）</summary>
    internal struct RedisWrite
    {
        /// <summary>操作类型（写入 / 删除）</summary>
        public RedisOp Op;

        /// <summary>目标 Hash Key：{CacheName}.{TypeName} 或 {CacheName}.Map.{MapName}</summary>
        public string Key;

        /// <summary>目标 Hash field：实体的 CacheObjectUID，或 Map 的字典 key</summary>
        public string Field;

        /// <summary>Upsert 时的完整对象 JSON（序列化在入队时完成，捕获变更时刻快照）</summary>
        public string Json;
    }

    /// <summary>
    /// Redis 同步服务：
    ///  1. 写路径 —— 无界 Channel + 单消费者批量执行，同 (key, field) 的多次写合并为一次；
    ///  2. 一致性 —— 每次写入通过 Lua 原子完成 HSET/HDEL + PUBLISH，订阅方收到通知后回读 Redis
    ///     最新值再合并（修复旧版"字段级 HUPT 与整体 JSON 反序列化不对称"的问题）；
    ///  3. 可靠性 —— 写入失败自动重试；连接恢复后触发全表对账合并，弥补 Pub/Sub 丢消息；
    ///  4. 高吞吐 —— 远端变更批量应用：按 (Key,Field) 去重 + 按 Key 合并 HMGET，
    ///     N 条消息只花 K 个 RTT（K = 涉及的 Key 数），订阅泵永不被网络 IO 阻塞。
    /// </summary>
    internal sealed class RedisSyncService : IDisposable
    {
        /// <summary>每个 Hash 上的版本计数字段名（HINCRBY 递增，仅观测用，加载时跳过）</summary>
        internal const string VersionField = "VERSION";

        // ---------------------------------------------------------------------------------
        // Lua 脚本：写入 + 版本号递增 + 发布，三条命令在 Redis 内原子执行。
        //
        // 参数（UpsertScript）：
        //   KEYS[1] = Hash Key
        //   ARGV[1] = field（UID 或 Map key）
        //   ARGV[2] = 完整对象 JSON
        //   ARGV[3] = 发布频道名
        //   ARGV[4] = 发布方实例 UID（订阅端据此过滤自己的消息）
        //
        // ⚠ 关键约束：cjson.encode 的字段名必须与订阅端 RemoteMessage 的属性名一致
        //   （Sender/Option/Key/Field），否则 Newtonsoft 按名匹配失败、消息被整体丢弃
        //   —— 这是上线前用真实双实例验证过的坑，改任何一边都要同步改另一边。
        // ---------------------------------------------------------------------------------
        internal const string UpsertScript = @"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[1], 'VERSION', 1)
redis.call('PUBLISH', ARGV[3], cjson.encode({ Sender = ARGV[4], Option = 'U', Key = KEYS[1], Field = ARGV[1] }))";

        // ---------------------------------------------------------------------------------
        // 删除脚本（参数同上，少一个 JSON 值参数）：
        //   KEYS[1] = Hash Key，ARGV[1] = field，ARGV[2] = 频道名，ARGV[3] = 发布方实例 UID
        // ---------------------------------------------------------------------------------
        internal const string DeleteScript = @"
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HINCRBY', KEYS[1], 'VERSION', 1)
redis.call('PUBLISH', ARGV[2], cjson.encode({ Sender = ARGV[3], Option = 'D', Key = KEYS[1], Field = ARGV[1] }))";

        /// <summary>
        /// 远端变更通知消息（订阅频道上的 JSON 载荷）。
        /// 只携带"定位符"，不携带数据本体 —— 全系统只有 Redis Hash 一种数据形态，
        /// 消息体积恒定（~100B），大实体也不会撑爆订阅端 output buffer。
        /// </summary>
        private sealed class RemoteMessage
        {
            /// <summary>发布方实例 UID（用于防回声：自己发的消息自己不处理）</summary>
            public long Sender { get; set; }

            /// <summary>操作类型："U" = 写入/更新，"D" = 删除</summary>
            public string Option { get; set; }

            /// <summary>目标 Hash Key（{CacheName}.{TypeName} 或 {CacheName}.Map.{name}）</summary>
            public string Key { get; set; }

            /// <summary>目标 Hash field（实体 UID 或 Map 字典 key）</summary>
            public string Field { get; set; }
        }

        // ------------------------------ 内部状态 ------------------------------

        private readonly MRedisCacheClient _client;              // 所属缓存客户端
        private readonly ConnectionMultiplexer _mux;            // Redis 连接复用器（自动重连）
        private readonly IDatabase _db;                         // 默认数据库操作入口
        private readonly string _channelName;                   // 变更发布/订阅频道
        private readonly Channel<RedisWrite> _writes;           // 写通道（业务线程 → 写线程）
        private readonly Channel<RemoteMessage> _applies;       // 应用通道（订阅泵 → 应用线程）
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _worker;                          // 写线程任务
        private readonly Task _applyWorker;                     // 远端变更应用线程任务
        private readonly ChannelMessageQueue _subscription;     // 频道订阅队列
        private readonly ConcurrentQueue<RemoteMessage> _pendingDuringLoad = new ConcurrentQueue<RemoteMessage>();
        private readonly int _batchSize;                        // 单批写入上限（Options.RedisBatchSize）
        private int _disposed;                                  // Dispose 幂等守卫（0=未关闭，1=已关闭）

        /// <summary>单批远端变更应用上限（去重后）。批越大 RTT 摊薄越充分，应用时延略增。</summary>
        private const int ApplyBatchMax = 256;

        /// <summary>
        /// 构造并启动服务：连接 Redis → 启动写线程 → 启动应用线程 → 订阅频道 → 挂接重连事件。
        /// </summary>
        /// <param name="client">所属缓存客户端</param>
        /// <param name="options">缓存配置（RedisEndpoints / RedisBatchSize）</param>
        /// <exception cref="RedisConnectionException">Redis 连接失败时抛出</exception>
        public RedisSyncService(MRedisCacheClient client, MRedisCacheOptions options)
        {
            _client = client;
            _batchSize = Math.Max(1, options.RedisBatchSize);
            _channelName = $"MRedisCache.DataChanged.{client.Store.CacheName}";

            // 连接配置：
            //   AbortOnConnectFail = false —— 初始连接失败不抛异常，后续由多路复用器自动重连，
            //   这是"断线期间写请求排队、恢复后继续"的关键前提；
            //   ExponentialRetry(5000) —— 重连间隔从 5 秒起步指数退避，避免风暴。
            var config = new ConfigurationOptions
            {
                SyncTimeout = 30000,
                AsyncTimeout = 30000,
                ConnectTimeout = 15000,
                KeepAlive = 45000,
                AbortOnConnectFail = false,
                ReconnectRetryPolicy = new ExponentialRetry(5000)
            };
            // 默认 DB（RedisDefaultDb）：本实例的 Hash 读写、Lua 执行与订阅
            // 全部经由 GetDatabase() 返回的同一个 IDatabase，因此只需在连接配置上
            // 指定 DefaultDatabase 即可全局生效；-1 = 不指定，沿用服务器默认（通常 db0）。
            if (options.RedisDefaultDb >= 0)
                config.DefaultDatabase = options.RedisDefaultDb;
            foreach (string endpoint in options.RedisEndpoints)
                config.EndPoints.Add(endpoint);

            _mux = ConnectionMultiplexer.Connect(config);
            _db = _mux.GetDatabase();

            // 写通道：无界队列 + 单消费者。
            // 业务线程只做 TryWrite（微秒级返回），Redis 写入的耗时/重试全部隔离在写线程。
            _writes = Channel.CreateUnbounded<RedisWrite>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            _worker = Task.Run(() => WriteLoopAsync(_cts.Token));

            // 远端变更应用通道：订阅回调只做解析+投递（立即返回，不阻塞订阅泵），
            // HMGET 批量回读与合并由独立应用线程执行。
            _applies = Channel.CreateUnbounded<RemoteMessage>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
            _applyWorker = Task.Run(() => ApplyLoopAsync(_cts.Token));

            // 订阅远端变更（RedisChannel.Literal 显式声明字面量语义，消除隐式转换过时警告）
            _subscription = _mux.GetSubscriber().Subscribe(RedisChannel.Literal(_channelName));
            _subscription.OnMessage(OnSubscriptionMessage);

            // 连接恢复 → 对账（弥补断线期间丢失的发布消息）
            _mux.ConnectionRestored += OnConnectionRestored;

            Log.Info($"Redis 已连接（{string.Join(",", options.RedisEndpoints)}），默认 DB = {config.DefaultDatabase?.ToString() ?? "服务器默认"}，订阅频道 {_channelName}。");
        }

        #region 写入入口（业务线程调用，全部非阻塞）

        /// <summary>
        /// 投递一条实体写入（整对象 JSON）。
        /// 在调用线程上完成序列化以捕获变更时刻快照，随后立即返回。
        /// </summary>
        /// <param name="obj">已进入缓存的对象（IsInCache = true）</param>
        internal void EnqueueUpsert(CacheObject obj)
        {
            Enqueue(new RedisWrite
            {
                Op = RedisOp.Upsert,
                Key = $"{_client.Store.CacheName}.{obj.TableName()}",
                Field = obj.CacheObjectUID.ToString(),
                Json = JsonUtil.To(obj)
            });
        }

        /// <summary>
        /// 投递一条实体删除（HDEL 单 field）。
        /// 无需序列化，只带定位符。
        /// </summary>
        /// <param name="obj">待删除对象（只需 UID 与表名有效）</param>
        internal void EnqueueDelete(CacheObject obj)
        {
            Enqueue(new RedisWrite
            {
                Op = RedisOp.Delete,
                Key = $"{_client.Store.CacheName}.{obj.TableName()}",
                Field = obj.CacheObjectUID.ToString()
            });
        }

        /// <summary>投递一条 CacheMap 写入（key → 值 JSON）。</summary>
        /// <param name="redisKey">Map 的完整 Redis Key（{CacheName}.Map.{name}）</param>
        /// <param name="field">Map 字典 key</param>
        /// <param name="json">值 JSON（调用方 CacheMap 已序列化）</param>
        internal void EnqueueMapUpsert(string redisKey, string field, string json)
        {
            Enqueue(new RedisWrite { Op = RedisOp.Upsert, Key = redisKey, Field = field, Json = json });
        }

        /// <summary>投递一条 CacheMap 删除。</summary>
        /// <param name="redisKey">Map 的完整 Redis Key</param>
        /// <param name="field">Map 字典 key</param>
        internal void EnqueueMapDelete(string redisKey, string field)
        {
            Enqueue(new RedisWrite { Op = RedisOp.Delete, Key = redisKey, Field = field });
        }

        /// <summary>统一投递入口：队列已关闭（Dispose 后）时丢弃并告警。</summary>
        private void Enqueue(RedisWrite write)
        {
            if (!_writes.Writer.TryWrite(write))
                Log.Warn("Redis 写队列已关闭，写入被丢弃。");
        }

        #endregion

        #region 写线程（单消费者：攒批 → 合并 → 批量 Lua 执行 → 失败重试）

        /// <summary>
        /// 写线程主循环：
        ///   1. WaitToReadAsync 挂起等待新写入（空闲零 CPU，替代旧版 0.1ms 自旋轮询）；
        ///   2. 攒批：最多取 _batchSize 条，按 (Key + Field) 字典去重 ——
        ///      同一对象在批内的多次修改只保留最后一次（写合并，削减 Redis 压力）；
        ///   3. 批量执行 Lua（含失败重试）。
        /// </summary>
        private async Task WriteLoopAsync(CancellationToken token)
        {
            Log.Info("Redis 写线程已启动。");
            var batch = new Dictionary<string, RedisWrite>();
            while (!token.IsCancellationRequested)
            {
                try
                {
                    // 无消息时异步挂起；通道关闭（Complete 调用）时返回 false 退出
                    if (!await _writes.Reader.WaitToReadAsync(token)) break;

                    batch.Clear();
                    // 同一 (key, field) 的多次写合并为最后一次（写合并，削减 Redis 压力）
                    while (batch.Count < _batchSize && _writes.Reader.TryRead(out RedisWrite write))
                        batch[write.Key + "\u0001" + write.Field] = write;

                    await ExecuteWithRetryAsync(batch.Values.ToList(), token);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    // 兜底：批量执行内部已处理业务异常，这里只防御意外错误，退避后继续
                    Log.Error("Redis 批量写入异常", ex);
                    try { await Task.Delay(3000, token); } catch (OperationCanceledException) { break; }
                }
            }
            Log.Info("Redis 写线程退出。");
        }

        /// <summary>
        /// 执行一批写入；失败无限重试（3 秒间隔），关闭时中止并丢弃剩余写入。
        /// 逐条构造 Lua 调用后 Task.WhenAll 并行提交，由 StackExchange.Redis 合并底层管线。
        /// </summary>
        /// <param name="writes">已合并的一批写请求</param>
        /// <param name="token">关闭令牌</param>
        private async Task ExecuteWithRetryAsync(List<RedisWrite> writes, CancellationToken token)
        {
            while (true)
            {
                try
                {
                    var tasks = new List<Task>(writes.Count);
                    foreach (RedisWrite write in writes)
                    {
                        if (write.Op == RedisOp.Upsert)
                        {
                            // Upsert：ARGV = [field, 完整JSON, 频道名, 本实例UID]
                            tasks.Add(_db.ScriptEvaluateAsync(UpsertScript,
                                new RedisKey[] { write.Key },
                                new RedisValue[] { write.Field, write.Json, _channelName, _client.InstanceUid.ToString() }));
                        }
                        else
                        {
                            // Delete：ARGV = [field, 频道名, 本实例UID]
                            tasks.Add(_db.ScriptEvaluateAsync(DeleteScript,
                                new RedisKey[] { write.Key },
                                new RedisValue[] { write.Field, _channelName, _client.InstanceUid.ToString() }));
                        }
                    }
                    await Task.WhenAll(tasks);
                    return; // 整批成功
                }
                catch (Exception ex)
                {
                    // 关闭过程中失败：不再重试，剩余写入丢弃（Close 语义优先）
                    if (token.IsCancellationRequested)
                    {
                        Log.Warn($"关闭过程中 Redis 写入中止，剩余 {writes.Count} 条被丢弃。");
                        return;
                    }
                    // 运行期失败（网络/连接）：保留整批，3 秒后重试 —— 保证"写入最终到达"
                    Log.Error($"Redis 写入失败，3 秒后重试（剩余 {writes.Count} 条）", ex);
                    try { await Task.Delay(3000, token); } catch (OperationCanceledException) { return; }
                }
            }
        }

        #endregion

        #region 订阅（远端变更，批量应用）

        /// <summary>
        /// 订阅泵回调（StackExchange.Redis 逐条串行调用）：
        ///   解析 → 校验 → 防回声 → （初始加载未完成则缓冲，否则）投递到应用通道。
        /// 全程无网络 IO，微秒级返回，保证订阅泵不被阻塞、消息不积压在 Redis 客户端缓冲区。
        /// </summary>
        private void OnSubscriptionMessage(ChannelMessage message)
        {
            try
            {
                // 1. 解析消息（字段名与 Lua cjson.encode 输出严格一致，见脚本注释）
                RemoteMessage msg = JsonUtil.From<RemoteMessage>(message.Message.ToString());
                if (msg == null || string.IsNullOrEmpty(msg.Key))
                {
                    Log.Warn($"无法解析的 Redis 消息: {message.Message}");
                    return;
                }

                // 2. 防回声：自己发布的消息直接忽略（消息可能来自同进程其他 Client，属正常）
                if (msg.Sender == _client.InstanceUid) return;

                // 3. 初始加载未完成：缓冲，等 CompleteInitialLoad 后统一转入应用队列。
                //    （Client 内置 2 秒兜底定时器，防 flag 翻转与投递之间的竞态导致滞留）
                if (!_client.InitialLoadCompleted)
                {
                    _pendingDuringLoad.Enqueue(msg);
                    return;
                }

                // 4. 只投递不处理：网络 IO 全部集中在应用线程批量执行
                if (!_applies.Writer.TryWrite(msg))
                    Log.Warn("远端变更应用队列已关闭，消息被丢弃。");
            }
            catch (Exception ex)
            {
                Log.Error("处理 Redis 订阅消息失败", ex);
            }
        }

        /// <summary>
        /// 把初始加载期间缓冲的全部远端变更转入应用队列。
        /// 由 MRedisCacheClient.CompleteInitialLoad() 主动调用 + Client 内 2 秒兜底定时器调用。
        /// </summary>
        /// <returns>本次转入的消息条数</returns>
        internal int DrainPendingDuringLoad()
        {
            int count = 0;
            while (_pendingDuringLoad.TryDequeue(out RemoteMessage msg))
            {
                _applies.Writer.TryWrite(msg);
                count++;
            }
            if (count > 0) Log.Info($"初始加载期间缓冲的 {count} 条远端变更已进入应用队列。");
            return count;
        }

        /// <summary>
        /// 应用线程主循环：攒批 → 去重 → 按表 HMGET 批量回读 → 顺序应用。
        /// 空闲时 WaitToReadAsync 挂起零 CPU；突发流量自动攒批，RTT 摊薄。
        /// </summary>
        private async Task ApplyLoopAsync(CancellationToken token)
        {
            Log.Info("远端变更应用线程已启动。");
            var batch = new List<RemoteMessage>(ApplyBatchMax);
            while (!token.IsCancellationRequested)
            {
                try
                {
                    // 无消息挂起；通道关闭时退出
                    if (!await _applies.Reader.WaitToReadAsync(token)) break;

                    // 攒批：取当前可用的全部消息（上限 ApplyBatchMax）
                    batch.Clear();
                    while (batch.Count < ApplyBatchMax && _applies.Reader.TryRead(out RemoteMessage msg))
                        batch.Add(msg);

                    if (batch.Count > 0)
                        await ApplyBatchAsync(batch, token);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    // 兜底防御：批量处理内部已分类捕获，这里只防意外错误
                    Log.Error("远端变更批量应用异常", ex);
                }
            }
            Log.Info("远端变更应用线程退出。");
        }

        /// <summary>
        /// 应用一批远端变更（核心三步）：
        ///   1. 按 (Key, Field) 去重 —— 同一对象多次变更只保留最后一次操作：
        ///      后到的 U 靠回读拿到最终态，后到的 D 代表对象已被删除，语义均收敛于 Redis 当前状态；
        ///   2. 按 Key 分组 HMGET 批量回读 —— K 个 Key 仅 K 个 RTT（替代逐条 HGET 的 N 个 RTT）；
        ///   3. 按到达顺序应用 —— 跨对象顺序无关（各自独立合并），同对象已由去重保证终态。
        /// </summary>
        private async Task ApplyBatchAsync(List<RemoteMessage> batch, CancellationToken token)
        {
            // 1. 按 (Key, Field) 去重，只保留每个对象的最后一次操作：
            //    同一对象连发多次变更，只需回读一次、应用最终态
            var dedup = new Dictionary<string, RemoteMessage>(batch.Count);
            foreach (RemoteMessage msg in batch)
                dedup[msg.Key + "\u0001" + msg.Field] = msg;

            // 2. 按 Key 分组，U 操作合并为每 Key 一次 HMGET（一个 RTT 取回整批对象）
            Dictionary<string, Dictionary<string, string>> jsonPerKey;
            try
            {
                jsonPerKey = await FetchBatchAsync(dedup.Values);
            }
            catch (Exception ex)
            {
                // 回读失败（网络/连接问题）：整批重新入队，3 秒后重试，不丢消息
                Log.Error($"远端变更批量回读失败，3 秒后重试（{dedup.Count} 条）", ex);
                foreach (RemoteMessage msg in dedup.Values)
                    _applies.Writer.TryWrite(msg);
                try { await Task.Delay(3000, token); } catch (OperationCanceledException) { }
                return;
            }

            // 3. 按到达顺序应用（单条失败只影响自身，不影响整批）
            foreach (RemoteMessage msg in dedup.Values)
            {
                if (token.IsCancellationRequested) return;
                Apply(msg, jsonPerKey);
            }
        }

        /// <summary>
        /// 按 Key 分组 HMGET 批量回读：K 个 Key = K 个 RTT（而非 N 条消息 N 个 RTT）。
        /// 多 Key 之间 Task.WhenAll 并行提交；单 Key 多 field 合并为一次 HMGET（集群安全：
        /// 同一 Hash Key 的多 field 必然落在同一 slot）。
        /// </summary>
        /// <param name="messages">去重后的消息集合</param>
        /// <returns>Key → (field → JSON) 的两级字典；D 操作无条目</returns>
        private async Task<Dictionary<string, Dictionary<string, string>>> FetchBatchAsync(IEnumerable<RemoteMessage> messages)
        {
            // 分组：只回读 U 操作（D 操作无需取值，直接本地删除）
            var fieldsPerKey = new Dictionary<string, List<string>>();
            foreach (RemoteMessage msg in messages)
            {
                if (msg.Option != "U") continue; // D 操作无需回读
                List<string> fields;
                if (!fieldsPerKey.TryGetValue(msg.Key, out fields))
                    fieldsPerKey[msg.Key] = fields = new List<string>();
                fields.Add(msg.Field);
            }

            var result = new Dictionary<string, Dictionary<string, string>>();
            if (fieldsPerKey.Count == 0) return result;

            // 每个 Key 一次 HashGetAsync，并行提交
            var keys = new List<string>(fieldsPerKey.Count);
            var fetchTasks = new List<Task<RedisValue[]>>(fieldsPerKey.Count);
            foreach (KeyValuePair<string, List<string>> kvp in fieldsPerKey)
            {
                keys.Add(kvp.Key);
                RedisValue[] fields = kvp.Value.Select(f => (RedisValue)f).ToArray();
                fetchTasks.Add(_db.HashGetAsync(kvp.Key, fields));
            }
            RedisValue[][] values = await Task.WhenAll(fetchTasks);

            // 还原为 Key → (field → JSON)；Redis 侧已删除的 field 值为 null（由调用方按删除处理）
            for (int i = 0; i < keys.Count; i++)
            {
                var keyValues = new Dictionary<string, string>();
                List<string> fields = fieldsPerKey[keys[i]];
                for (int j = 0; j < fields.Count; j++)
                    keyValues[fields[j]] = values[i][j].HasValue ? values[i][j].ToString() : null;
                result[keys[i]] = keyValues;
            }
            return result;
        }

        /// <summary>
        /// 应用单条远端变更（在应用线程上串行调用）。
        /// 路由顺序：先查 Map 注册表（完整 Key 匹配），再按表名查实体 Manager（O(1)）。
        /// </summary>
        /// <param name="msg">去重后的变更消息</param>
        /// <param name="jsonPerKey">批量回读结果（U 操作的值都在这里）</param>
        private void Apply(RemoteMessage msg, Dictionary<string, Dictionary<string, string>> jsonPerKey)
        {
            try
            {
                // 1. Map 类型：{CacheName}.Map.{name}（完整 Key 精确匹配）
                CacheMapInfo map = _client.Maps.Find(msg.Key);
                if (map != null)
                {
                    if (msg.Option == "D")
                    {
                        map.Delete(msg.Field);
                        return;
                    }
                    string mapJson = GetFetched(jsonPerKey, msg);
                    if (!string.IsNullOrEmpty(mapJson)) map.Upsert(msg.Field, mapJson);
                    else map.Delete(msg.Field); // 回读为空 = Redis 侧已被删除，本地同步删除
                    return;
                }

                // 2. 实体类型：{CacheName}.{TypeName} → 按表名找 Manager（O(1)，无广播）
                ICacheManager manager = _client.Store.FindManager(ExtractTableName(msg.Key));
                if (manager == null)
                {
                    // 本实例没有装配该类型的 Manager（例如工具进程只缓存部分表），安全忽略
                    Log.Debug($"收到未注册类型的远端变更，已忽略: {msg.Key}/{msg.Field}");
                    return;
                }

                // 实体的 Field 是 UID 字符串；解析失败说明消息异常，忽略
                long uid;
                if (!long.TryParse(msg.Field, out uid)) return;

                if (msg.Option == "D")
                {
                    manager.OnRemoteDelete(uid);
                    return;
                }

                // 回读 Redis 最新值再合并：天然合并同一对象的多次变更，
                // 且消息不携带数据副本，全系统只有 Redis hash 一种数据形态
                string json = GetFetched(jsonPerKey, msg);
                if (!string.IsNullOrEmpty(json))
                    manager.OnRemoteUpsert(uid, json);
                else
                    manager.OnRemoteDelete(uid); // 回读为空 = 已被后续变更删除
            }
            catch (Exception ex)
            {
                // 单条失败只影响自身：记录后继续处理同批其余消息
                Log.Error($"应用远端变更失败: {msg.Key}/{msg.Field}", ex);
            }
        }

        /// <summary>从批量回读结果中取指定消息的 JSON（两级字典查找，缺失返回 null）</summary>
        private static string GetFetched(Dictionary<string, Dictionary<string, string>> jsonPerKey, RemoteMessage msg)
        {
            Dictionary<string, string> values;
            string json;
            return jsonPerKey != null
                && jsonPerKey.TryGetValue(msg.Key, out values)
                && values.TryGetValue(msg.Field, out json)
                ? json : null;
        }

        /// <summary>
        /// 从 Redis Key 提取表名：取最后一个 '.' 之后的部分。
        /// （CacheName 允许包含 '.'，因为解析永远只看最后一段）
        /// </summary>
        private static string ExtractTableName(string redisKey)
        {
            int idx = redisKey.LastIndexOf('.');
            return idx >= 0 ? redisKey.Substring(idx + 1) : redisKey;
        }

        #endregion

        #region 读接口（Manager 加载 / 读穿透使用）

        /// <summary>
        /// 读取单个 Hash field（同步）。
        /// 供 Manager 的 Get(uid) 读穿透使用；失败返回 null（由调用方按 miss 处理）。
        /// </summary>
        /// <returns>field 的字符串值；不存在或失败返回 null</returns>
        internal string HashGet(string key, string field)
        {
            try
            {
                RedisValue value = _db.HashGet(key, field);
                return value.HasValue ? value.ToString() : null;
            }
            catch (Exception ex)
            {
                Log.Error($"HashGet {key}/{field} 失败", ex);
                return null;
            }
        }

        /// <summary>
        /// 读取整个 Hash（同步）。
        /// 供 Manager 启动加载 / 对账合并、CacheMap 加载使用；
        /// 结果包含 VERSION 字段，调用方需跳过。失败返回 null。
        /// </summary>
        /// <returns>field → value 字典；失败返回 null</returns>
        internal Dictionary<string, string> HashGetAll(string key)
        {
            try
            {
                HashEntry[] entries = _db.HashGetAll(key);
                var result = new Dictionary<string, string>(entries.Length);
                foreach (HashEntry entry in entries)
                    result[entry.Name.ToString()] = entry.Value.ToString();
                return result;
            }
            catch (Exception ex)
            {
                Log.Error($"HashGetAll {key} 失败", ex);
                return null;
            }
        }

        #endregion

        /// <summary>
        /// Redis 连接恢复事件：Pub/Sub 在断线期间丢失的消息无法重放，
        /// 因此延迟 5 秒调度一次全表对账合并（避开重连抖动期），把偏差收敛回来。
        /// </summary>
        private void OnConnectionRestored(object sender, ConnectionFailedEventArgs e)
        {
            Log.Warn("Redis 连接已恢复，调度一次全表对账合并（弥补断线期间丢失的发布消息）。");
            _client.ScheduleReconciliation();
        }

        /// <summary>
        /// 关闭服务：取消令牌 → 等待写线程/应用线程退出（各最多 10 秒，超时强关）→ 退订 → 释放连接。
        /// 关键修复：旧版 terminate 对忙等线程 Join 可能永久挂死；这里全部带超时兜底。
        /// </summary>
        public void Dispose()
        {
            // Interlocked 幂等守卫：重复 Dispose 直接返回
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            try
            {
                _cts.Cancel();
                if (!_worker.Wait(TimeSpan.FromSeconds(10)))
                    Log.Warn("Redis 写线程 10 秒内未退出，强制关闭（在途写入可能丢失）。");
                if (!_applyWorker.Wait(TimeSpan.FromSeconds(10)))
                    Log.Warn("远端变更应用线程 10 秒内未退出，强制关闭。");
                _subscription.Unsubscribe();
                _mux.Dispose();
            }
            catch (Exception ex)
            {
                Log.Error("关闭 RedisSyncService 异常", ex);
            }
        }
    }
}
