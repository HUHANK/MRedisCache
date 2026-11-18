using System;
using Newtonsoft.Json;

// =====================================================================================
// 模块：JsonUtil —— 统一的 JSON 序列化入口（全库唯一）
//
// 关键修复：旧版 MCache 的 FromJson / Extension.ToJson 各自创建了
// JsonSerializerSettings 却没有传给 JsonConvert 调用（写了等于没写），
// 导致序列化配置全部失效、多态类型写出后读不回。
// 本实现：settings 静态只读一份，所有调用统一传入，序列化行为全库一致。
//
// 设计约定：
//   - TypeNameHandling.None：缓存数据走强类型泛型（CacheManager&lt;T&gt;），不需要类型名注入，
//     也避免旧序列化多态带来的兼容包袱；
//   - NullValueHandling.Ignore：跳过 null 字段，减小 Redis 存储；
//   - 异常不静默：解析失败记 Error 日志后返回 null（调用方按 miss 处理）。
// =====================================================================================

namespace MRedisCache.Internal
{
    /// <summary>
    /// 统一的 JSON 序列化入口。
    /// 关键修复：旧版 FromJson/Extension.ToJson 创建了 JsonSerializerSettings
    /// 却没有传给 JsonConvert 调用，导致多态类型写出后读不回；本实现统一传入。
    /// </summary>
    internal static class JsonUtil
    {
        /// <summary>全局唯一序列化配置（所有读写共用，保证行为一致）</summary>
        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.None,       // 缓存数据走强类型泛型，不需要多态类型名
            NullValueHandling = NullValueHandling.Ignore,  // 跳过 null 字段，减小 Redis 存储
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        /// <summary>
        /// 序列化为 JSON 字符串。
        /// 失败返回空串（调用方以空串作为"无法序列化"信号并记日志）。
        /// </summary>
        /// <typeparam name="T">对象类型</typeparam>
        /// <param name="obj">待序列化对象（null 返回空串）</param>
        public static string To<T>(T obj)
        {
            if (ReferenceEquals(obj, null)) return string.Empty;
            try { return JsonConvert.SerializeObject(obj, Settings); }
            catch (Exception ex) { Log.Error("JSON 序列化失败", ex); return string.Empty; }
        }

        /// <summary>
        /// 反序列化为强类型对象。
        /// 空串/解析失败返回 null（调用方按 miss 处理）；失败记 Error 日志，不静默。
        /// </summary>
        /// <typeparam name="T">目标类型（必须为引用类型）</typeparam>
        /// <param name="json">JSON 字符串</param>
        public static T From<T>(string json) where T : class
        {
            if (string.IsNullOrEmpty(json)) return null;
            try { return JsonConvert.DeserializeObject<T>(json, Settings); }
            catch (Exception ex) { Log.Error($"JSON 反序列化失败: {ex.Message}"); return null; }
        }

        /// <summary>
        /// 把 JSON 合并填充到已有对象（远端变更应用 OnRemoteUpsert / Add 冲突合并 ApplyOnto）。
        /// 只覆盖 JSON 中出现的属性；null 字段因 Ignore 配置不会覆盖。
        /// 调用方需自行处理回声抑制（BeginRemoteApply/EndRemoteApply）。
        /// </summary>
        /// <param name="target">目标对象（属性 setter 会触发）</param>
        /// <param name="json">JSON 字符串</param>
        public static void Populate(object target, string json)
        {
            if (target == null || string.IsNullOrEmpty(json)) return;
            JsonConvert.PopulateObject(json, target, Settings);
        }
    }
}
