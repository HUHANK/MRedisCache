using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using MRedisCache.Internal;

// =====================================================================================
// 模块：CacheMap&lt;V&gt; —— Redis 同步的字符串字典（Key-Value 型缓存）
//
// 定位：与 CacheManager（实体型，按主键索引）互补的轻量 KV 缓存，
//       适合"值没有主键概念、整体读写"的场景（如 临时任务表、会话状态、运行时参数）。
//
// 数据形态：Redis Hash Key = {CacheName}.Map.{MapName}，field = 字典 key，value = 值 JSON。
//
// 同步语义：
//   本地 Add/Remove/Clear → 异步投递 Redis 写（经 Lua 原子发布，其他实例实时同步）；
//   远端变更 → CacheMapRegistry 路由（完整 Key 精确匹配）→ OnRemoteUpsert/OnRemoteDelete。
//
// 相对旧版 MCache（CacheMap）的关键修复：
//   Clear() 先快照 keys → 清内存 → 逐个投递 Redis 删除；
//   （旧版先 _data.Clear() 再遍历已空的字典删 Redis，导致 Redis 数据永久残留。）
//
// 使用方式：client.GetMap&lt;V&gt;(name) 获取；同名 Map 不允许更换 V 类型。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// CacheMap 的远端路由信息。
    /// 存在意义：解决泛型类型擦除 —— RedisSyncService 持有非泛型的委托句柄，
    /// 收到远端变更后按完整 Redis Key 查表调用，无需反射。
    /// </summary>
    internal sealed class CacheMapInfo
    {
        /// <summary>远端写入/更新回调：(field, json)</summary>
        public Action<string, string> Upsert;

        /// <summary>远端删除回调：(field)</summary>
        public Action<string> Delete;

        /// <summary>对账时全量重载回调（清空本地后从 Redis 重读）</summary>
        public Action Reload;
    }

    /// <summary>
    /// CacheMap 注册表：完整 Redis Key → 路由信息。
    /// 由 MRedisCacheClient.GetMap 创建 Map 时注册；RedisSyncService 收到远端消息后查表路由。
    /// </summary>
    internal sealed class CacheMapRegistry
    {
        private readonly ConcurrentDictionary<string, CacheMapInfo> _maps =
            new ConcurrentDictionary<string, CacheMapInfo>();

        /// <summary>按完整 Redis Key 查找（找不到返回 null，调用方按"未注册"处理）</summary>
        public CacheMapInfo Find(string redisKey)
        {
            return _maps.TryGetValue(redisKey, out CacheMapInfo info) ? info : null;
        }

        /// <summary>注册（同 Key 重复注册时后者覆盖前者）</summary>
        public void Register(string redisKey, CacheMapInfo info)
        {
            _maps[redisKey] = info;
        }

        /// <summary>全部注册项（Client 对账遍历用）</summary>
        public IEnumerable<CacheMapInfo> All() => _maps.Values;
    }

    /// <summary>
    /// Redis 同步的字符串字典（Key-Value 型缓存，值为任意可序列化引用类型）。
    /// 通过 client.GetMap&lt;V&gt;(name) 获取，不要直接 new。
    /// </summary>
    /// <typeparam name="V">值类型（JSON 序列化约束：必须为引用类型）</typeparam>
    public sealed class CacheMap<V> : IEnumerable<KeyValuePair<string, V>> where V : class
    {
        private readonly MRedisCacheClient _client;
        private readonly ConcurrentDictionary<string, V> _data = new ConcurrentDictionary<string, V>();

        /// <summary>Map 名称（同一 Client 内唯一）</summary>
        public string Name { get; }

        /// <summary>本 Map 在 Redis 上的 Hash Key：{CacheName}.Map.{Name}</summary>
        internal string RedisKey => $"{_client.Store.CacheName}.Map.{Name}";

        /// <summary>
        /// 内部构造：注册前先从 Redis 全量加载（对账时也可 ReloadFromRedis）。
        /// </summary>
        internal CacheMap(MRedisCacheClient client, string name)
        {
            _client = client;
            Name = name;
            LoadFromRedis();
        }

        /// <summary>
        /// 从 Redis 全量加载（构造时调用）。
        /// 逐 key 反序列化覆盖本地；跳过 VERSION 观测字段；加载失败静默（保持空字典）。
        /// </summary>
        private void LoadFromRedis()
        {
            if (_client.Redis == null) return;
            Dictionary<string, string> all = _client.Redis.HashGetAll(RedisKey);
            if (all == null) return;
            foreach (KeyValuePair<string, string> kvp in all)
            {
                if (kvp.Key == RedisSyncService.VersionField) continue;
                V value = JsonUtil.From<V>(kvp.Value);
                if (value != null) _data[kvp.Key] = value;
            }
        }

        /// <summary>读取指定 key 的值；不存在返回 default(V)（即 null）</summary>
        public V Get(string key)
        {
            return _data.TryGetValue(key, out V value) ? value : default(V);
        }

        /// <summary>尝试读取（标准 TryGet 模式）</summary>
        public bool TryGetValue(string key, out V value) => _data.TryGetValue(key, out value);

        /// <summary>是否包含指定 key</summary>
        public bool ContainsKey(string key) => _data.ContainsKey(key);

        /// <summary>全部字典 key（O(1) 视图，遍历由调用方控制）</summary>
        public IEnumerable<string> Keys => _data.Keys;

        /// <summary>全部值</summary>
        public IEnumerable<V> Values => _data.Values;

        /// <summary>有效条目数量</summary>
        public int Count => _data.Count;

        /// <summary>索引器：读（miss 返回 null）/ 写（等同 Add，同步 Redis）</summary>
        public V this[string key]
        {
            get { return Get(key); }
            set { Add(key, value); }
        }

        /// <summary>
        /// 写入/覆盖一个条目：先更新本地，再投递 Redis 异步写
        /// （经 Lua 原子 HSET + PUBLISH，其他实例实时收到通知）。
        /// </summary>
        /// <param name="key">字典 key（空 key 忽略）</param>
        /// <param name="value">值（null 时序列化为空串，建议避免）</param>
        public void Add(string key, V value)
        {
            if (string.IsNullOrEmpty(key)) return;
            _data[key] = value;
            _client.Redis?.EnqueueMapUpsert(RedisKey, key, JsonUtil.To(value));
        }

        /// <summary>不存在才写入（存在则什么都不做，也不同步 Redis）</summary>
        public void AddIfNotExist(string key, V value)
        {
            if (ContainsKey(key)) return;
            Add(key, value);
        }

        /// <summary>
        /// 删除一个条目：先删本地，成功才投递 Redis 删除（经 Lua 原子 HDEL + PUBLISH）。
        /// </summary>
        /// <returns>true = 删除成功；false = key 不存在</returns>
        public bool Remove(string key)
        {
            if (!_data.TryRemove(key, out _)) return false;
            _client.Redis?.EnqueueMapDelete(RedisKey, key);
            return true;
        }

        /// <summary>
        /// 清空全部条目。
        /// 关键修复：旧版先 Clear 本地字典再遍历（已空的）字典删 Redis，导致 Redis 数据残留。
        /// 现在的执行顺序：先快照 keys → 清本地内存 → 逐个投递 Redis 删除。
        /// </summary>
        public void Clear()
        {
            List<string> keys = _data.Keys.ToList();   // ① 先快照
            _data.Clear();                             // ② 再清内存
            foreach (string key in keys)               // ③ 最后删 Redis（用快照的 keys）
                _client.Redis?.EnqueueMapDelete(RedisKey, key);
        }

        #region 远端变更 / 对账（RedisSyncService / Client 调用）

        /// <summary>
        /// 远端写入/更新（Redis 应用线程调用）：
        /// 反序列化回读的 JSON 后覆盖本地（不回声 —— 本方法不投递 Redis 写）。
        /// </summary>
        internal void OnRemoteUpsert(string key, string json)
        {
            V value = JsonUtil.From<V>(json);
            if (value != null) _data[key] = value;
        }

        /// <summary>远端删除（Redis 应用线程调用）：本地直接移除。</summary>
        internal void OnRemoteDelete(string key)
        {
            _data.TryRemove(key, out _);
        }

        /// <summary>
        /// 对账重载（Client.SafeReconcile 调用）：
        /// 清空本地后从 Redis 全量重读 —— 以 Redis 为准收敛，
        /// 弥补 Pub/Sub 丢消息期间错过的 Map 变更。
        /// </summary>
        internal void ReloadFromRedis()
        {
            _data.Clear();
            LoadFromRedis();
        }

        #endregion

        /// <summary>枚举器直通内部字典（线程安全快照式遍历，ConcurrentDictionary 语义）</summary>
        public IEnumerator<KeyValuePair<string, V>> GetEnumerator() => _data.GetEnumerator();
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
