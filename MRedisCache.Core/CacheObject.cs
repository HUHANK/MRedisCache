using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using MRedisCache.Internal;
using SqlSugar;

// =====================================================================================
// 模块：CacheObject —— 缓存实体基类（业务实体的唯一必须继承点）
//
// 职责：
//   1. 身份：CacheObjectUID（全缓存唯一、set-once 保护）+ 时间戳（创建/更新）；
//   2. 写入口：SetProperty —— 业务实体所有属性 setter 必须经由它赋值，
//      变更自动进入「自定义索引维护 → Redis 异步写 → 脏数据落库」三个通道；
//   3. 状态标记：IsNew / IsDirty / IsDeleted（落库分类依据，仅框架内部修改）；
//   4. 远端应用：BeginRemoteApply / EndRemoteApply —— 应用远端变更时抑制回声
//      （不再把收到的数据发回 Redis）并收集字段变化供索引维护。
//
// 层级选择（通过继承哪个基类决定）：
//   CacheLocalObject        —— 仅本地内存（临时数据、单进程内共享）；
//   CacheRedisObject        —— 本地 + Redis（跨实例共享、无持久化需求）；
//   CacheRedisDatabaseObject —— 本地 + Redis + 数据库（write-behind 落库）。
//
// 相对旧版 MCache 的关键修复：
//   1. 通知不再挂静态事件广播，而是 Owner（CacheStore）按表名路由到唯一 Manager，
//      杜绝旧版"构造函数 + SetInCache 双重订阅、每次变更执行两遍"的问题；
//   2. 所有变更统一走整对象 JSON 写 Redis，不再有字段级/整体级两套不对称协议；
//   3. 远端应用期间自动抑制回声，不会把收到的数据再发回 Redis 造成风暴。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>缓存层级（决定数据同步到哪几层）</summary>
    public enum CacheLevel
    {
        /// <summary>仅本地内存：变更不外发，适合单进程临时数据</summary>
        Local = 0,

        /// <summary>本地内存 + Redis：跨实例实时共享，不落库</summary>
        Redis = 1,

        /// <summary>本地内存 + Redis + 数据库：跨实例共享 + write-behind 落库</summary>
        RedisDatabase = 2
    }

    /// <summary>
    /// 缓存实体基类。
    /// 业务实体通过 <see cref="SetProperty{T}"/> 修改字段，变更会自动进入
    /// 自定义索引维护 → Redis 异步写入 → 脏数据落库 三个通道。
    /// </summary>
    public abstract class CacheObject
    {
        // ---------------- 身份与状态 ----------------

        /// <summary>所属缓存仓库（进入缓存后由 Manager 设置，变更路由的入口）</summary>
        [JsonIgnore] internal CacheStore Owner { get; set; }

        private long _uid;                       // 对象唯一 ID（set-once）
        private readonly object _flagLock = new object();  // 状态标志锁（读写都短暂，纳秒级）
        private bool _isNew = true;              // 新建标记 → 落库走 INSERT
        private bool _isDirty;                   // 脏标记 → 落库走 UPDATE
        private bool _isDeleted;                 // 删除标记 → 落库走 DELETE / 查询不可见
        private bool _suppressNotify;            // 远端应用中：抑制变更通知（防回声）
        private List<KeyValuePair<string, object>> _remoteChanges;  // 远端应用期间收集的字段变化

        /// <summary>
        /// 对象唯一 ID（63 位正数，时间戳+随机+序列）。
        /// set-once 语义：首次赋值生效；再次赋不同值只告警不覆盖
        /// （防止反序列化/合并意外篡改身份）。UID 同时是 Redis Hash 的 field。
        /// </summary>
        [JsonIgnore]
        [SugarColumn(IsIgnore = true)]           // 数据库不存 UID 列（按主键回源）
        public long CacheObjectUID
        {
            get { lock (_flagLock) { return _uid; } }
            set
            {
                lock (_flagLock)
                {
                    if (_uid == value) return;
                    if (_uid != 0)
                    {
                        Log.Warn($"CacheObjectUID 已固定为 {_uid}，忽略新值 {value}。");
                        return;
                    }
                    if (value <= 0) return;
                    _uid = value;
                }
            }
        }

        /// <summary>对象创建时间（Unix 秒）。随 JSON 序列化持久化到 Redis；数据库忽略。</summary>
        [SugarColumn(IsIgnore = true)] public long CacheCreateTime { get; set; } = UnixTime.NowSeconds();

        /// <summary>最近更新时间（Unix 秒）。实例间合并的判据（旧不盖新），依赖 NTP 大致同步。</summary>
        [SugarColumn(IsIgnore = true)] public long CacheUpdateTime { get; set; } = UnixTime.NowSeconds();

        /// <summary>缓存层级（由所继承的基类构造函数决定，序列化时忽略）</summary>
        [JsonIgnore] public CacheLevel Level { get; internal set; }

        /// <summary>是否已进入缓存（true 时属性变更才会触发通知）</summary>
        [JsonIgnore] public bool IsInCache { get; internal set; }

        /// <summary>新建标记：true = 尚未落库，落库线程走 INSERT；成功后由框架清除</summary>
        [JsonIgnore] public bool IsNew { get { lock (_flagLock) { return _isNew; } } }

        /// <summary>脏标记：true = 有未落库的修改，落库线程走 UPDATE；成功后由框架清除</summary>
        [JsonIgnore] public bool IsDirty { get { lock (_flagLock) { return _isDirty; } } }

        /// <summary>删除标记：true = 对查询不可见；RedisDatabase 层级下等 DB 删除成功才移出内存</summary>
        [JsonIgnore] public bool IsDeleted { get { lock (_flagLock) { return _isDeleted; } } }

        /// <summary>缓存表名（默认取实体类型名，与 Redis Key、Manager 路由对应）</summary>
        public string TableName() => GetType().Name;

        /// <summary>
        /// 属性修改入口：子类属性 setter 必须经由本方法赋值。
        ///
        /// 执行顺序：
        ///   1. 值相等 → 直接返回 false（零开销，避免无效变更风暴）；
        ///   2. 赋值 + 更新 CacheUpdateTime；
        ///   3. 远端应用中（_suppressNotify）：不通知（防回声），
        ///      但把 (属性名, 旧值) 记入变更收集列表，供 Manager 维护自定义索引；
        ///   4. 正常路径：IsInCache 且有 Owner 时，通知所属 Manager
        ///      → 索引维护 → 脏队列 → Redis 异步写（全部 O(1)，无网络阻塞）。
        ///
        /// ⚠ 直接给私有字段赋值（如 _name = x）不会触发任何同步，属使用错误。
        /// </summary>
        /// <typeparam name="T">属性类型</typeparam>
        /// <param name="field">属性 backing field（ref 传递）</param>
        /// <param name="value">新值</param>
        /// <param name="propertyName">属性名（CallerMemberName 自动填充）</param>
        /// <returns>true = 值发生变化并已进入同步通道；false = 值未变化</returns>
        protected bool SetProperty<T>(ref T field, T value, [CallerMemberName] string propertyName = null)
        {
            if (EqualityComparer<T>.Default.Equals(field, value)) return false;
            T oldValue = field;
            field = value;
            CacheUpdateTime = UnixTime.NowSeconds();

            if (_suppressNotify)
            {
                // 远端变更应用期间：不回声，但记录字段变化，供 Manager 维护自定义索引
                if (_remoteChanges != null)
                    _remoteChanges.Add(new KeyValuePair<string, object>(propertyName, oldValue));
                return true;
            }

            if (IsInCache && Owner != null)
                Owner.NotifyFieldChanged(this, propertyName, oldValue);
            return true;
        }

        #region 状态标记（仅框架内部使用：Manager / 落库线程调用）

        /// <summary>分配新的唯一 UID（Add 时对象尚无 UID 使用）</summary>
        internal void AssignNewUid() { CacheObjectUID = UniqueIdGenerator.Next(); }

        /// <summary>标记为脏（字段变更路径使用，落库走 UPDATE）</summary>
        internal void MarkDirty() { lock (_flagLock) { _isDirty = true; } }

        /// <summary>标记为新建（落库走 INSERT）</summary>
        internal void MarkNew() { lock (_flagLock) { _isNew = true; _isDirty = true; } }

        /// <summary>
        /// 落库成功后清除脏标志。
        /// 关键修复：旧版在执行 SQL 之前就清标志，批量失败即丢数据；
        /// 本实现由 CacheManager.FlushUids 在 SqlSugar 执行成功后才调用。
        /// </summary>
        internal void MarkSaved() { lock (_flagLock) { _isNew = false; _isDirty = false; } }

        /// <summary>标记删除（查询不可见；RedisDatabase 层级下等 DB 删除成功才移出内存）</summary>
        internal void MarkDeleted() { lock (_flagLock) { _isDeleted = true; } }

        #endregion

        #region 远端变更应用（抑制回声 + 收集字段变化）

        /// <summary>
        /// 开始应用远端变更（Manager.OnRemoteUpsert 调用）：
        /// 置抑制标志 + 新建变更收集列表。此后到 EndRemoteApply 之间，
        /// Populate 触发的所有属性变更都不会回声 Redis。
        /// </summary>
        internal void BeginRemoteApply()
        {
            _suppressNotify = true;
            _remoteChanges = new List<KeyValuePair<string, object>>();
        }

        /// <summary>
        /// 结束远端变更应用：解除抑制并返回期间收集的字段变化列表
        /// [(属性名, 旧值), ...]，供 Manager 的 UpdateCustomIndex 维护自定义索引。
        /// </summary>
        internal List<KeyValuePair<string, object>> EndRemoteApply()
        {
            _suppressNotify = false;
            List<KeyValuePair<string, object>> changes = _remoteChanges;
            _remoteChanges = null;
            return changes;
        }

        #endregion
    }

    /// <summary>仅本地内存缓存实体基类：变更不外发，适合单进程临时数据</summary>
    public abstract class CacheLocalObject : CacheObject
    {
        public CacheLocalObject() { Level = CacheLevel.Local; }
    }

    /// <summary>本地内存 + Redis 缓存实体基类：跨实例实时共享，无数据库持久化</summary>
    public abstract class CacheRedisObject : CacheObject
    {
        public CacheRedisObject() { Level = CacheLevel.Redis; }
    }

    /// <summary>本地内存 + Redis + 数据库缓存实体基类：跨实例共享 + write-behind 落库</summary>
    public abstract class CacheRedisDatabaseObject : CacheObject
    {
        public CacheRedisDatabaseObject() { Level = CacheLevel.RedisDatabase; }
    }
}
