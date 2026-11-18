using System;

// =====================================================================================
// 模块：Attributes —— 缓存行为特性（标注在实体类/属性上的声明式配置）
//
// 与 SqlSugar 特性的分工：
//   [SugarTable] / [SugarColumn] —— 负责「数据库映射」（表名、主键、列、长度等）；
//   本模块特性                —— 负责「缓存行为」（加载、删除、过期）。
//   其中主键有双通道识别：SugarColumn(IsPrimaryKey) 与 CachePrimaryKey 等价，
//   推荐用前者（一个特性两处生效）。
//
// 未标注任何特性时的默认行为：
//   主键 = ID/Id 惯例属性；LoadOnStartup = true；SyncDeleteToDatabase = true；
//   EvictExpired = false；ExpiredSeconds = 86400（一天）。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// 主键标记。与 SqlSugar 的 <c>[SugarColumn(IsPrimaryKey = true)]</c> 二选一，两者都会被识别；
    /// 都未标注时回退到名为 ID / Id 的属性。
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class CachePrimaryKeyAttribute : Attribute
    {
    }

    /// <summary>
    /// 缓存行为配置（标注在实体类上）。
    /// 各项只影响 RedisDatabase 层级（继承 CacheRedisDatabaseObject）的实体行为，
    /// 除 EvictExpired/ExpiredSeconds 对所有层级生效。
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class CacheBehaviorAttribute : Attribute
    {
        /// <summary>
        /// 程序启动时是否从数据库全量加载该表（仅 RedisDatabase 层级生效）。
        /// 加载语义：只补 Redis/内存缺失的行，不覆盖已有数据（Redis 比 DB 新）。
        /// 默认 true；大表且允许按需回源（Get(probe)）时可设为 false。
        /// </summary>
        public bool LoadOnStartup { get; set; } = true;

        /// <summary>
        /// 删除内存数据时是否同步删除数据库数据（仅 RedisDatabase 层级生效）。
        /// true（默认）：Remove 先删缓存，DB DELETE 由落库线程批量执行，
        /// 成功前对象对查询不可见（IsDeleted 隐藏态）；
        /// false：只删缓存和 Redis，数据库保留（适合"逻辑删除自己做"的场景）。
        /// </summary>
        public bool SyncDeleteToDatabase { get; set; } = true;

        /// <summary>
        /// 是否启用过期自动淘汰（所有层级生效）。
        /// 淘汰条件：距 CacheUpdateTime 超过 <see cref="ExpiredSeconds"/> 且非脏非新非删；
        /// 淘汰动作：移出内存 + 删 Redis；数据库保留（过期 ≠ 删除）。
        /// 由落库线程低优先级执行（限流 EvictBatchLimit）。
        /// </summary>
        public bool EvictExpired { get; set; } = false;

        /// <summary>过期时间（秒），按 CacheUpdateTime 计算，默认一天。仅 EvictExpired=true 时生效。</summary>
        public int ExpiredSeconds { get; set; } = 60 * 60 * 24;
    }
}
