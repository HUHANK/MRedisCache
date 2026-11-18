using System.Collections.Generic;
using SqlSugar;

// =====================================================================================
// 模块：MRedisCacheOptions —— 缓存配置项（MRedisCacheClient.Create 的唯一入参）
//
// 分四组：数据库（SqlSugar）/ Redis / 落库节奏 / 一致性与淘汰。
// 所有项都有合理默认值；最小可用配置只需 CacheName。
// 纯内存模式：EnableDatabase = false 且 RedisEndpoints 为空。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// MRedisCache 配置项。
    /// </summary>
    public sealed class MRedisCacheOptions
    {
        // ---------------- 基础 ----------------

        /// <summary>
        /// 缓存名（Redis Key 前缀 / 发布频道后缀）。
        /// 同一 Redis 中不同缓存必须不同名（同名会共享数据并互相通知）；
        /// 允许包含 '.'（Key 解析只看最后一段）。
        /// </summary>
        public string CacheName { get; set; } = "MRedisCache";

        // ---------------- 数据库（SqlSugar） ----------------

        /// <summary>
        /// 是否启用数据库。true 时 RedisDatabase 层级实体自动获得落库与回源能力；
        /// 必须同时提供 <see cref="ConnectionString"/>。
        /// </summary>
        public bool EnableDatabase { get; set; } = false;

        /// <summary>数据库类型（SqlSugar 原生支持 SqlServer/MySQL/Oracle/PostgreSQL 等）</summary>
        public DbType DbType { get; set; } = DbType.SqlServer;

        /// <summary>数据库连接串（按 DbType 语法）</summary>
        public string ConnectionString { get; set; } = "";

        // ---------------- Redis ----------------

        /// <summary>
        /// Redis 节点列表（"host:port" 格式，支持多节点）。
        /// 为空 = 不启用 Redis（纯本地模式，Redis/RedisDatabase 层级退化为本地内存）。
        /// </summary>
        public List<string> RedisEndpoints { get; set; } = new List<string>();

        /// <summary>
        /// Redis 单批写入条数（默认 300）。
        /// 批内按 (Key, Field) 去重合并 —— 同一对象多次修改只写最后一次；
        /// 批越大吞吐越高、单条时延略增。
        /// </summary>
        public int RedisBatchSize { get; set; } = 300;

        // ---------------- 落库 ----------------

        /// <summary>
        /// 落库线程轮询间隔（毫秒，默认 1000）。
        /// write-behind 的数据丢失窗口 ≈ 本间隔 + 批量排队时间；
        /// 进程崩溃时最多丢这个窗口内的数据库变更（Redis 里仍有，重启后重新落库）。
        /// </summary>
        public int FlushIntervalMs { get; set; } = 1000;

        /// <summary>单表单轮落库批量上限（默认 300，SqlSugar 批量语句的行数）</summary>
        public int FlushBatchSize { get; set; } = 300;

        // ---------------- 一致性与淘汰 ----------------

        /// <summary>
        /// 定期对账间隔（秒，默认 300，0 = 关闭）。
        /// 对账 = 全表 HGET_ALL 按 CacheUpdateTime 合并 + 全部 Map 全量重载，
        /// 用于弥补 Pub/Sub at-most-once 语义下丢消息造成的实例间偏差。
        /// 建议保持开启。
        /// </summary>
        public int ReconcileIntervalSeconds { get; set; } = 300;

        /// <summary>
        /// 单轮过期淘汰的全局上限（默认 500）。
        /// 淘汰在落库线程的周期内低优先级执行，限流避免挤占落库
        /// （继承旧版 500 条/次的预算设计）。
        /// </summary>
        public int EvictBatchLimit { get; set; } = 500;
    }
}
