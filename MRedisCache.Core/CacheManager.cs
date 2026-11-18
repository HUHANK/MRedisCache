using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using MRedisCache.Internal;

// =====================================================================================
// 模块：CacheManager&lt;T&gt; —— 实体类型的缓存管理器（业务侧主要交互面）
//
// 职责：
//   1. 存储：持有本表全部对象的内存字典（TableStore.Data）+ 主键索引 + 自定义二级索引；
//   2. 读穿透：内存 → Redis（Get(uid)）→ 数据库（Get(probe)）逐级降级，miss 自动回填；
//   3. 写路径：Add/Remove/字段变更 → 索引维护 + Redis 异步写 + 脏数据落库；
//   4. 远端合并：接收其他实例的变更通知，按 CacheUpdateTime 旧不盖新合并；
//   5. 落库：把脏队列中的对象按 Insert/Update/Delete 分类批量写入数据库。
//
// 获取方式（二选一，全进程对同一实体类型只允许装配一次）：
//   1. client.GetManager&lt;T&gt;() —— 无自定义索引时使用；
//   2. new YourManager(client) —— 有自定义索引时继承本类后直接构造，
//      之后 client.GetManager&lt;T&gt;() 会返回同一实例。
//
// 关键设计：
//   - 装配守卫用 TableStore.BindManager()（Interlocked），修复旧版"实例字段守卫
//     导致每次 new 都重复加载 + 静态事件重复挂接"的泄漏问题；
//   - 远端应用走 BeginRemoteApply/EndRemoteApply：抑制回声（不再把收到的数据发回 Redis），
//     同时收集字段变化供自定义索引维护。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// 实体类型的缓存管理器：内存字典 + 主键索引 + 自定义二级索引 + 读穿透 + 写路径。
    /// </summary>
    /// <typeparam name="T">缓存实体类型（必须有无参构造函数）</typeparam>
    public class CacheManager<T> : ICacheManager where T : CacheObject, new()
    {
        // ---------------- 类级元数据（每个封闭泛型类型只计算一次） ----------------

        /// <summary>主键属性列表（识别 SugarColumn(IsPrimaryKey) / CachePrimaryKey / ID 惯例）</summary>
        private static readonly List<PropertyInfo> PrimaryKeys;

        /// <summary>缓存行为配置（[CacheBehavior] 特性，未标注时取默认值）</summary>
        private static readonly CacheBehaviorAttribute Behavior;

        // ---------------- 实例状态 ----------------

        /// <summary>所属缓存客户端</summary>
        private readonly MRedisCacheClient _client;

        /// <summary>本表存储单元（数据字典 + 脏队列，与其他 Manager 共享同一实例）</summary>
        private readonly TableStore _table;

        /// <summary>主键值字符串 → UID 的索引（复合主键用 | 连接）</summary>
        private readonly ConcurrentDictionary<string, long> _primaryKeyIndex = new ConcurrentDictionary<string, long>();

        /// <summary>静态构造：加载实体元数据（主键 / 行为配置），只执行一次</summary>
        static CacheManager()
        {
            PrimaryKeys = EntityMeta.GetPrimaryKeys(typeof(T));
            Behavior = EntityMeta.GetBehavior(typeof(T));
        }

        /// <summary>
        /// 构造即装配：绑定表存储 → 注册自身 → 从 Redis 加载 → 从数据库补缺。
        ///
        /// 执行顺序（对应架构文档 3.1 节第 2 步）：
        ///   1. Store.GetTable 取/建 TableStore；
        ///   2. BindManager 原子装配守卫 —— 已有同类型 Manager 则抛异常
        ///      （关键修复：旧版守卫是实例字段，每次 new 都重复加载 + 重复挂接静态事件）；
        ///   3. 设置表级配置（过期淘汰）+ 注册到 Store 的类型注册表；
        ///   4. LoadFromRedis：HGET_ALL 全量合并（对账时也复用本方法）；
        ///   5. LoadOnStartup 且启用数据库时 LoadFromDatabase：只补缺，不覆盖已有
        ///      （Redis 的数据比数据库新，回源 DB 只为填补 Redis 没有的行）。
        ///
        /// 初始加载失败不抛异常（记录日志后继续运行），避免单表故障拖垮整个进程启动。
        /// </summary>
        /// <param name="client">所属缓存客户端</param>
        /// <exception cref="InvalidOperationException">同一实体类型重复装配 Manager</exception>
        public CacheManager(MRedisCacheClient client)
        {
            if (client == null) throw new ArgumentNullException(nameof(client));
            _client = client;
            _table = client.Store.GetTable(TableName);

            // 原子装配守卫：同一张表只允许一个 Manager 完成装配
            if (!_table.BindManager())
                throw new InvalidOperationException(
                    $"Manager<{TableName}> 已存在，请通过 MRedisCacheClient.GetManager<{typeof(T).Name}>() 获取，不要重复创建。");

            _table.Manager = this;
            _table.EvictExpired = Behavior.EvictExpired;
            _table.ExpiredSeconds = Behavior.ExpiredSeconds;
            _client.Store.RegisterManager(typeof(T), this);

            try
            {
                Stopwatch sw = Stopwatch.StartNew();
                LoadFromRedis();
                if (Behavior.LoadOnStartup && client.Persistence != null)
                    LoadFromDatabase();
                sw.Stop();
                Log.Info($"Manager<{TableName}> 装配完成，共 {_table.Data.Count} 条，耗时 {sw.ElapsedMilliseconds}ms。");
            }
            catch (Exception ex)
            {
                Log.Error($"Manager<{TableName}> 初始加载失败（可运行，数据可能不完整）", ex);
            }
        }

        /// <summary>缓存表名 = 实体类型名（与 Redis Key、Manager 路由对应）</summary>
        public string TableName => typeof(T).Name;

        /// <summary>本表在 Redis 上的 Hash Key：{CacheName}.{TypeName}</summary>
        internal string RedisKey => $"{_client.Store.CacheName}.{TableName}";

        #region 查询（读穿透：内存 → Redis → 数据库）

        /// <summary>
        /// 按 UID 查询（读穿透第 1 级）：
        ///   内存命中且未删除 → 返回；
        ///   miss → ReadThroughRedis 回读 Redis（此时才可能接触网络）。
        ///   数据库不存 UID 列，按 UID 不回源 DB（按主键回源请用 Get(probe)）。
        /// </summary>
        /// <param name="uid">对象唯一 ID</param>
        /// <returns>缓存对象；不存在或已删除返回 null</returns>
        public T Get(long uid)
        {
            if (_table.Data.TryGetValue(uid, out CacheObject obj))
                return obj.IsDeleted ? null : (T)obj;
            return ReadThroughRedis(uid);
        }

        /// <summary>
        /// 按主键值字符串查询（复合主键为 "a|b" 格式）。
        /// 只查内存与 Redis 建立的主键索引；主键 miss 需要回源数据库时请用 <see cref="Get(T)"/>。
        /// </summary>
        /// <param name="key">主键值字符串</param>
        /// <returns>缓存对象；不存在返回 null</returns>
        public T GetByPrimaryKey(string key)
        {
            if (string.IsNullOrEmpty(key)) return null;
            if (_primaryKeyIndex.TryGetValue(key, out long uid))
                return Get(uid);
            return null;
        }

        /// <summary>
        /// 按主键探测对象查询（读穿透完整链路）：
        ///   内存（主键索引）→ miss → 数据库 SELECT（SqlSugar 按主键表达式查询）
        ///   → 命中后 MarkSaved 并放入缓存。
        /// probe 只需填好主键字段，其余字段忽略。
        /// </summary>
        /// <param name="probe">主键探测对象</param>
        /// <returns>缓存对象（查询命中时为缓存中的实例）；miss 返回 null</returns>
        public T Get(T probe)
        {
            if (probe == null) return null;
            T local = GetByPrimaryKey(EntityMeta.GetPrimaryKeyValue(probe));
            if (local != null) return local;

            // 无数据库（纯内存/Redis 模式）或查询失败 → null
            if (_client.Persistence == null) return null;
            try
            {
                T row = _client.Persistence.QueryByPrimaryKeys(probe, PrimaryKeys);
                if (row == null) return null;

                // 回源命中：清除新建标志（数据库已有此行），放入缓存
                row.MarkSaved();
                if (row.CacheObjectUID == 0) row.AssignNewUid();
                Attach(row);
                return row;
            }
            catch (Exception ex)
            {
                Log.Error($"Manager<{TableName}> 回源查询失败", ex);
                return null;
            }
        }

        /// <summary>
        /// Get(uid) miss 后的 Redis 读穿透：
        /// HGET 单 field → 反序列化 → MarkSaved → 放入缓存。
        /// 回读失败/不存在返回 null（调用方按 miss 处理，数据库按 UID 不可查）。
        /// </summary>
        private T ReadThroughRedis(long uid)
        {
            if (_client.Redis == null) return null;
            string json = _client.Redis.HashGet(RedisKey, uid.ToString());
            T obj = JsonUtil.From<T>(json);
            if (obj == null) return null;
            obj.CacheObjectUID = uid;
            obj.MarkSaved();

            // 主键已存在说明内存有更新数据（例如远端刚合并过），不覆盖
            if (!_primaryKeyIndex.ContainsKey(EntityMeta.GetPrimaryKeyValue(obj)))
                Attach(obj);
            return obj;
        }

        #endregion

        #region 写入 / 删除

        /// <summary>
        /// 新增数据。若主键已存在，则用新值整体覆盖既有对象（等同 Update 语义，
        /// 不做字段级合并 —— 见架构文档 6.2 已知限制）。
        /// 执行顺序：
        ///   1. 计算主键值，无主键拒绝；
        ///   2. 主键已存在 → ApplyOnto 整体覆盖；
        ///   3. 否则：分配 UID → 入内存字典 + 主键索引 + 自定义索引
        ///      →（RedisDatabase 层级）入脏队列 →（非 Local 层级）投递 Redis 异步写。
        /// </summary>
        /// <param name="value">新对象</param>
        /// <returns>true = 新增或覆盖成功；false = 参数为空或无主键</returns>
        public bool Add(T value)
        {
            if (value == null) return false;

            string pk = EntityMeta.GetPrimaryKeyValue(value);
            if (string.IsNullOrEmpty(pk))
            {
                Log.Warn($"Manager<{TableName}>.Add：实体没有可用主键，已拒绝。");
                return false;
            }

            // 主键已存在：用新值整体覆盖既有对象（等同 Update）
            if (_primaryKeyIndex.TryGetValue(pk, out long existUid) &&
                _table.Data.TryGetValue(existUid, out CacheObject existObj) &&
                !existObj.IsDeleted)
            {
                ApplyOnto((T)existObj, value);
                return true;
            }

            // 全新对象：分配 UID → 放入内存与索引 → 进入写通道
            if (value.CacheObjectUID == 0) value.AssignNewUid();
            value.Owner = _client.Store;
            value.IsInCache = true;
            value.MarkNew();                       // IsNew=true → 落库时走 INSERT
            _table.Data[value.CacheObjectUID] = value;
            _primaryKeyIndex[pk] = value.CacheObjectUID;
            AddCustomIndex(value);

            if (value.Level == CacheLevel.RedisDatabase)
                _table.MarkDirty(value.CacheObjectUID);   // 等待落库线程批量 INSERT
            if (value.Level != CacheLevel.Local)
                _client.Redis?.EnqueueUpsert(value);      // 异步写 Redis + 发布通知
            return true;
        }

        /// <summary>
        /// 把 value 的内容合并到已有对象 target（Add 的主键冲突路径）。
        /// 复用远端应用机制：BeginRemoteApply 抑制回声（Populate 触发的属性变更
        /// 不会再发回 Redis），同时收集字段变化维护自定义索引。
        /// 合并后仍手动标记脏 + 投递 Redis（本路径是本地写，需要向外传播）。
        /// </summary>
        private void ApplyOnto(T target, T value)
        {
            string json = JsonUtil.To(value);
            target.BeginRemoteApply();
            JsonUtil.Populate(target, json);
            List<KeyValuePair<string, object>> changes = target.EndRemoteApply();
            foreach (KeyValuePair<string, object> change in changes)
                UpdateCustomIndex(target, change.Key, change.Value);
            target.CacheUpdateTime = UnixTime.NowSeconds();

            if (target.Level == CacheLevel.RedisDatabase)
            {
                target.MarkDirty();
                _table.MarkDirty(target.CacheObjectUID);
            }
            if (target.Level != CacheLevel.Local)
                _client.Redis?.EnqueueUpsert(target);
        }

        /// <summary>按 UID 删除对象（详见 <see cref="RemoveInternal"/> 的删除语义）。</summary>
        /// <param name="uid">对象 UID</param>
        /// <returns>true = 删除成功；false = 对象不存在</returns>
        public bool Remove(long uid)
        {
            if (!_table.Data.TryGetValue(uid, out CacheObject obj)) return false;
            return RemoveInternal(obj);
        }

        /// <summary>
        /// 按对象删除：优先用 UID 定位，无 UID 时退化为按主键索引定位。
        /// </summary>
        /// <param name="value">待删除对象（只需 UID 或主键有效）</param>
        /// <returns>true = 删除成功；false = 未找到</returns>
        public bool Remove(T value)
        {
            if (value == null) return false;
            if (value.CacheObjectUID > 0 && _table.Data.ContainsKey(value.CacheObjectUID))
                return Remove(value.CacheObjectUID);
            long uid;
            return _primaryKeyIndex.TryGetValue(EntityMeta.GetPrimaryKeyValue(value), out uid) && Remove(uid);
        }

        /// <summary>
        /// 删除的统一执行路径（对应架构文档 3.5 节）：
        ///   1. 删主键索引 + 自定义索引（立即可查不到）；
        ///   2. 投递 Redis 删除（异步 HDEL + PUBLISH，缓存先删）；
        ///   3a. RedisDatabase 层级且配置同步删库：
        ///       仅标记 IsDeleted（对查询不可见），保留在 Data 中等落库线程
        ///       执行 DB DELETE 成功后才真正移出内存 —— 保证数据库删除不丢；
        ///   3b. 其他层级：立即移出内存。
        /// </summary>
        private bool RemoveInternal(CacheObject obj)
        {
            _primaryKeyIndex.TryRemove(EntityMeta.GetPrimaryKeyValue(obj), out _);
            RemoveCustomIndex((T)obj);

            // Redis 先删（异步）
            _client.Redis?.EnqueueDelete(obj);

            if (obj.Level == CacheLevel.RedisDatabase && Behavior.SyncDeleteToDatabase)
            {
                // 保留在 Data 中（IsDeleted=true，查询不可见），等数据库删除成功后由落库线程清理
                obj.MarkDeleted();
                _table.MarkDirty(obj.CacheObjectUID);
                return true;
            }

            obj.IsInCache = false;
            obj.MarkDeleted();
            _table.Data.TryRemove(obj.CacheObjectUID, out _);
            return true;
        }

        #endregion

        #region 集合视图

        /// <summary>
        /// 全部有效对象的枚举（惰性，过滤 IsDeleted）。
        /// 注意：O(n) 遍历，大表高频调用会占用 CPU（见架构文档 6.3）。
        /// </summary>
        public IEnumerable<T> Values
        {
            get
            {
                foreach (CacheObject obj in _table.Data.Values)
                    if (!obj.IsDeleted) yield return (T)obj;
            }
        }

        /// <summary>
        /// 有效对象数量。注意：O(n) 扫描（20 万条约 97ms），高频调用请自行缓存。
        /// </summary>
        public int Count
        {
            get
            {
                int count = 0;
                foreach (CacheObject obj in _table.Data.Values)
                    if (!obj.IsDeleted) count++;
                return count;
            }
        }

        /// <summary>是否存在指定 UID 的有效对象</summary>
        public bool ContainsKey(long uid)
        {
            return _table.Data.TryGetValue(uid, out CacheObject obj) && !obj.IsDeleted;
        }

        /// <summary>主键值字符串（复合主键用 | 连接），与 GetByPrimaryKey 的 key 格式一致</summary>
        public string GetPrimaryKeyValue(T value) => EntityMeta.GetPrimaryKeyValue(value);

        #endregion

        #region 自定义二级索引（子类按需覆写）

        /// <summary>
        /// 对象进入缓存时调用（Add / 回源 Attach / 远端新增）。子类在此添加自定义索引。
        /// </summary>
        protected virtual void AddCustomIndex(T value) { }

        /// <summary>
        /// 对象被移出缓存时调用（Remove / 过期淘汰 / 远端删除）。子类在此清理自定义索引。
        /// </summary>
        protected virtual void RemoveCustomIndex(T value) { }

        /// <summary>
        /// 对象字段变化时调用（本地 SetProperty 与远端 Populate 都会触发）。
        /// oldValue 为变更前的值，用于索引键的迁移。
        /// </summary>
        /// <param name="value">变更后的对象</param>
        /// <param name="fieldName">变化的属性名（nameof 语义）</param>
        /// <param name="oldValue">变更前的属性值</param>
        protected virtual void UpdateCustomIndex(T value, string fieldName, object oldValue) { }

        #endregion

        #region ICacheManager 路由实现（CacheStore / Redis 应用线程调用）

        /// <summary>
        /// 本地字段变更入口（CacheObject.SetProperty → CacheStore 路由到这里）：
        ///   1. 维护自定义索引；
        ///   2. RedisDatabase 层级 → 入脏队列（等落库）；
        ///   3. 非 Local 层级 → 投递 Redis 异步写。
        /// 该方法在业务线程上执行，全部操作 O(1)，无网络 IO。
        /// </summary>
        public void OnLocalFieldChanged(CacheObject obj, string fieldName, object oldValue)
        {
            UpdateCustomIndex((T)obj, fieldName, oldValue);
            if (obj.Level == CacheLevel.RedisDatabase)
                _table.MarkDirty(obj.CacheObjectUID);
            if (obj.Level != CacheLevel.Local)
                _client.Redis?.EnqueueUpsert(obj);
        }

        /// <summary>
        /// 远端新增/更新（Redis 应用线程调用）：
        ///   内存无此对象 → 反序列化后 Attach（MarkSaved 状态，不触发写回）；
        ///   内存已有 → CacheUpdateTime 旧不盖新；新则 Populate 整体覆盖：
        ///     BeginRemoteApply 抑制回声 + 收集字段变化 → EndRemoteApply →
        ///     用收集的变化维护自定义索引；主键变化时同步迁移主键索引。
        /// </summary>
        /// <param name="uid">对象 UID</param>
        /// <param name="json">Redis 回读的完整对象 JSON</param>
        public void OnRemoteUpsert(long uid, string json)
        {
            if (uid <= 0 || string.IsNullOrEmpty(json)) return;
            T incoming = JsonUtil.From<T>(json);
            if (incoming == null) return;

            // 已有对象：按 CacheUpdateTime 决定是否覆盖（旧不盖新）
            if (_table.Data.TryGetValue(uid, out CacheObject current))
            {
                if (incoming.CacheUpdateTime <= current.CacheUpdateTime) return;

                string pkOld = EntityMeta.GetPrimaryKeyValue(current);
                current.BeginRemoteApply();          // 抑制回声：Populate 期间属性变更不再发回 Redis
                JsonUtil.Populate(current, json);
                List<KeyValuePair<string, object>> changes = current.EndRemoteApply();
                current.CacheObjectUID = uid;        // 防御性恢复（Populate 不会覆盖 UID，set-once 保护）
                string pkNew = EntityMeta.GetPrimaryKeyValue(current);

                if (pkNew != pkOld)
                {
                    // 主键被远端修改：迁移主键索引
                    _primaryKeyIndex.TryRemove(pkOld, out _);
                    _primaryKeyIndex[pkNew] = uid;
                }
                foreach (KeyValuePair<string, object> change in changes)
                    UpdateCustomIndex((T)current, change.Key, change.Value);
                return;
            }

            // 新对象：直接放入内存（MarkSaved 状态，不需要写回 Redis/DB）
            incoming.CacheObjectUID = uid;
            incoming.MarkSaved();
            Attach(incoming);
        }

        /// <summary>
        /// 远端删除（Redis 应用线程调用）：删主键索引 + 自定义索引，移出内存。
        /// RedisDatabase 层级的远端删除不触发本地数据库操作（删除由发布方负责）。
        /// </summary>
        public void OnRemoteDelete(long uid)
        {
            if (!_table.Data.TryGetValue(uid, out CacheObject obj)) return;
            _primaryKeyIndex.TryRemove(EntityMeta.GetPrimaryKeyValue(obj), out _);
            RemoveCustomIndex((T)obj);
            obj.IsInCache = false;
            obj.MarkDeleted();
            _table.Data.TryRemove(uid, out _);
        }

        #endregion

        #region 加载 / 落库 / 淘汰（启动加载、落库线程、对账调用）

        /// <summary>
        /// 把对象放入内存（不触发 Redis / DB 写），供启动加载与读穿透回填使用：
        /// 分配 UID → 挂 Owner → 入数据字典 + 主键索引 + 自定义索引。
        /// 主键已存在时跳过（保留内存中更新的数据）。
        /// </summary>
        private void Attach(T obj)
        {
            string pk = EntityMeta.GetPrimaryKeyValue(obj);
            if (_primaryKeyIndex.ContainsKey(pk)) return;
            if (obj.CacheObjectUID == 0) obj.AssignNewUid();
            obj.Owner = _client.Store;
            obj.IsInCache = true;
            _table.Data[obj.CacheObjectUID] = obj;
            _primaryKeyIndex[pk] = obj.CacheObjectUID;
            AddCustomIndex(obj);
        }

        /// <summary>
        /// 从 Redis 全量加载并合并（构造函数 / 对账调用）。
        /// HGET_ALL 后逐条走 OnRemoteUpsert（即 CacheUpdateTime 旧不盖新合并语义），
        /// 因此对账时本地更新的数据不会被 Redis 旧值覆盖。
        /// 跳过 VERSION 观测字段。
        /// </summary>
        public void LoadFromRedis()
        {
            if (_client.Redis == null) return;
            Dictionary<string, string> all = _client.Redis.HashGetAll(RedisKey);
            if (all == null) return;
            foreach (KeyValuePair<string, string> kvp in all)
            {
                if (kvp.Key == RedisSyncService.VersionField) continue;
                long uid;
                if (!long.TryParse(kvp.Key, out uid)) continue;
                OnRemoteUpsert(uid, kvp.Value);
            }
        }

        /// <summary>
        /// 从数据库全量加载（构造函数调用；SqlSugar Queryable 全表查询）。
        /// 只补内存/Redis 缺失的行 —— Redis 的数据是 write-behind 的最新状态，
        /// 比回源数据库的新，因此 DB 行不覆盖已有数据（启动顺序：先 Redis 后 DB）。
        /// </summary>
        public void LoadFromDatabase()
        {
            if (_client.Persistence == null) return;
            Stopwatch sw = Stopwatch.StartNew();
            List<T> rows = _client.Persistence.QueryAll<T>();
            int added = 0;
            foreach (T row in rows)
            {
                string pk = EntityMeta.GetPrimaryKeyValue(row);
                if (_primaryKeyIndex.ContainsKey(pk)) continue; // 已有则不覆盖
                row.MarkSaved();
                if (row.CacheObjectUID == 0) row.AssignNewUid();
                Attach(row);
                added++;
            }
            sw.Stop();
            Log.Debug($"Manager<{TableName}> 从数据库补载 {added} 条（跳过已存在 {rows.Count - added} 条），耗时 {sw.ElapsedMilliseconds}ms。");
        }

        /// <summary>
        /// 把脏队列中的数据批量写入数据库（落库线程周期调用）。
        /// 循环取批直到取空或不足一批（保证突发积压尽快清完）。
        /// </summary>
        public void FlushDirty()
        {
            if (_client.Persistence == null) return;
            int batchSize = _client.Options.FlushBatchSize;
            List<long> uids = _table.TakeDirty(batchSize);
            while (uids.Count > 0)
            {
                FlushUids(uids);
                if (uids.Count < batchSize) break;
                uids = _table.TakeDirty(batchSize);
            }
        }

        /// <summary>
        /// 落库单批执行（对应架构文档 3.3 节第 7 步）：
        ///   1. 从内存字典捞出对象，按 IsNew / IsDirty / IsDeleted 分成三组；
        ///   2. SqlSugar 批量执行（同批三条 SQL：Insertable / Updateable / Deleteable）；
        ///   3. 成功后才 MarkSaved 清脏标志 / 移出已删除对象
        ///      （关键修复：旧版先清后写，失败即丢数据）；
        ///   4. 失败：记日志 + 整批 UID 重新入脏队列，下个周期重试
        ///      （IsDeleted 对象保持隐藏态持续重试，直到 DB 删除成功）。
        /// </summary>
        private void FlushUids(List<long> uids)
        {
            var inserts = new List<T>();
            var updates = new List<T>();
            var deletes = new List<T>();

            // 分拣：同一对象只属于一组（优先级 IsDeleted > IsNew > IsDirty）
            foreach (long uid in uids)
            {
                if (!_table.Data.TryGetValue(uid, out CacheObject obj)) continue;
                if (obj.IsDeleted)
                {
                    if (Behavior.SyncDeleteToDatabase) deletes.Add((T)obj);
                    continue;
                }
                if (obj.IsNew) inserts.Add((T)obj);
                else if (obj.IsDirty) updates.Add((T)obj);
            }

            if (inserts.Count == 0 && updates.Count == 0 && deletes.Count == 0) return;

            try
            {
                _client.Persistence.Execute(inserts, updates, deletes);

                // 关键修复：成功之后才清脏标志（旧版先清后写，失败即丢数据）
                foreach (T item in inserts) item.MarkSaved();
                foreach (T item in updates) item.MarkSaved();
                foreach (CacheObject deleted in deletes)
                {
                    // DB 删除成功，对象完成使命，真正移出内存
                    deleted.IsInCache = false;
                    _table.Data.TryRemove(deleted.CacheObjectUID, out _);
                }
                Log.Debug($"Manager<{TableName}> 落库成功：insert {inserts.Count} / update {updates.Count} / delete {deletes.Count}。");
            }
            catch (Exception ex)
            {
                // 失败：脏标志未清（Add/字段变更路径本来就标记着），把本轮 UID 重新入队，下个周期重试
                Log.Error($"Manager<{TableName}> 落库失败，将在下个周期重试（insert {inserts.Count} / update {updates.Count} / delete {deletes.Count}）", ex);
                foreach (long uid in uids) _table.MarkDirty(uid);
            }
        }

        /// <summary>
        /// 过期淘汰（落库线程同周期调用，低优先级）：
        ///   淘汰条件 = EvictExpired 开启 && 非脏非新非删 && 距 CacheUpdateTime 超过 ExpiredSeconds；
        ///   淘汰动作 = 移出内存 + 清索引 + 投递 Redis 删除（但不删数据库 —— 过期数据 DB 保留）。
        ///   budget 限流单轮全局淘汰量，避免挤占落库。
        /// </summary>
        /// <param name="budget">本轮允许淘汰的最大数量</param>
        /// <returns>实际淘汰数量</returns>
        public int ProcessExpired(int budget)
        {
            if (!_table.EvictExpired || budget <= 0) return 0;
            long now = UnixTime.NowSeconds();
            int removed = 0;
            foreach (KeyValuePair<long, CacheObject> kvp in _table.Data)
            {
                if (removed >= budget) break;

                CacheObject obj = kvp.Value;
                // 脏/新/删状态的对象跳过（正在等待落库，不能丢）
                if (obj.IsDeleted || obj.IsNew || obj.IsDirty) continue;
                if (now - obj.CacheUpdateTime <= _table.ExpiredSeconds) continue;

                if (_table.Data.TryRemove(kvp.Key, out _))
                {
                    _primaryKeyIndex.TryRemove(EntityMeta.GetPrimaryKeyValue(obj), out _);
                    RemoveCustomIndex((T)obj);
                    obj.IsInCache = false;
                    _client.Redis?.EnqueueDelete(obj);   // Redis 同步删除；数据库保留
                    removed++;
                }
            }
            return removed;
        }

        #endregion
    }
}
