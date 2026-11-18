// =====================================================================================
// 模块：ICacheManager —— Manager 的非泛型路由接口
//
// 存在意义：RedisSyncService / PersistenceService / CacheStore 都以"表名"工作，
// 不携带泛型参数。本接口是 CacheManager<T> 的类型擦除视图，
// 供框架内部按表名路由调用，取代旧版「静态事件 FieldDataChanged + 全 Manager 广播 +
// 每个接收方自己过滤表名」的模式（O(#Manager) → O(1)）。
//
// 调用方与线程：
//   OnLocalFieldChanged —— 业务线程（CacheObject.SetProperty → CacheStore 路由）；
//   OnRemoteUpsert / OnRemoteDelete —— Redis 应用线程（批量应用后逐条调用）；
//   LoadFromRedis —— Manager 构造（业务线程）/ 对账（定时器线程）；
//   LoadFromDatabase —— Manager 构造（业务线程）；
//   FlushDirty / ProcessExpired —— 落库线程（周期轮询）。
// =====================================================================================

namespace MRedisCache
{
    internal interface ICacheManager
    {
        /// <summary>缓存表名（= 实体类型名，Redis Key 最后一段，路由键）</summary>
        string TableName { get; }

        /// <summary>
        /// 本地字段变更（业务线程调用）：
        /// 维护自定义索引 →（RedisDatabase 层级）入脏队列 →（非 Local 层级）投递 Redis 异步写。
        /// 全部 O(1) 无网络 IO。
        /// </summary>
        /// <param name="obj">变更的对象</param>
        /// <param name="fieldName">变化的属性名</param>
        /// <param name="oldValue">变更前的值（索引迁移用）</param>
        void OnLocalFieldChanged(CacheObject obj, string fieldName, object oldValue);

        /// <summary>
        /// 远端新增/更新（Redis 应用线程调用）：
        /// 按 CacheUpdateTime 旧不盖新合并；Populate 覆盖时抑制回声并收集字段变化维护索引。
        /// </summary>
        /// <param name="uid">对象 UID</param>
        /// <param name="json">Redis 回读的完整对象 JSON</param>
        void OnRemoteUpsert(long uid, string json);

        /// <summary>
        /// 远端删除（Redis 应用线程调用）：删索引、移出内存。
        /// RedisDatabase 层级的远端删除不触发本地数据库操作（删除由发布方负责）。
        /// </summary>
        /// <param name="uid">对象 UID</param>
        void OnRemoteDelete(long uid);

        /// <summary>
        /// 从 Redis 全量加载并合并（Manager 构造 / 对账调用）。
        /// 合并语义 = OnRemoteUpsert（CacheUpdateTime 旧不盖新），对账时本地新数据不会被覆盖。
        /// </summary>
        void LoadFromRedis();

        /// <summary>
        /// 从数据库全量加载（Manager 构造调用）：只补内存/Redis 缺失的行，不覆盖已有数据。
        /// </summary>
        void LoadFromDatabase();

        /// <summary>把脏数据批量写入数据库（落库线程周期调用）。成功才清脏标志，失败重新入队。</summary>
        void FlushDirty();

        /// <summary>
        /// 处理过期淘汰（落库线程同周期调用，低优先级）。
        /// </summary>
        /// <param name="budget">本轮允许淘汰的最大数量</param>
        /// <returns>实际淘汰数量（调用方据此扣减全局预算）</returns>
        int ProcessExpired(int budget);
    }
}
