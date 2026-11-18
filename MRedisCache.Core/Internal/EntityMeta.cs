using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SqlSugar;

// =====================================================================================
// 模块：EntityMeta —— 实体元数据缓存（反射结果只算一次，热路径零反射）
//
// 缓存三样东西（均按 Type 键、ConcurrentDictionary 存储）：
//   1. PrimaryKeys：主键属性列表（识别优先级 SugarColumn(IsPrimaryKey) &gt;
//      CachePrimaryKey 特性 &gt; ID/Id 惯例属性）；
//   2. Behaviors：[CacheBehavior] 特性配置（未标注返回默认实例）；
//   3. PkValueGetters：主键值取值委托 —— 用 Expression 编译成
//      string.Join("|", pk1, pk2, ...) 的强类型委托，替代旧版每次反射。
//
// 相对旧版的性能修复：旧版主键值逐次反射计算、GetAllFields 每次分配元组列表；
// 本模块把全部反射成本压缩到类型首次使用时的一次性开销。
// =====================================================================================

namespace MRedisCache.Internal
{
    /// <summary>
    /// 实体元数据缓存：主键属性、缓存行为、主键值取值委托。
    /// 全部反射结果只计算一次并编译为委托，避免旧版每次反射 / 全表扫描的开销。
    /// </summary>
    internal static class EntityMeta
    {
        /// <summary>Type → 主键属性列表</summary>
        private static readonly ConcurrentDictionary<Type, List<PropertyInfo>> PrimaryKeys =
            new ConcurrentDictionary<Type, List<PropertyInfo>>();

        /// <summary>Type → 缓存行为配置</summary>
        private static readonly ConcurrentDictionary<Type, CacheBehaviorAttribute> Behaviors =
            new ConcurrentDictionary<Type, CacheBehaviorAttribute>();

        /// <summary>Type → 主键值取值委托（编译后的 string.Join）</summary>
        private static readonly ConcurrentDictionary<Type, Func<CacheObject, string>> PkValueGetters =
            new ConcurrentDictionary<Type, Func<CacheObject, string>>();

        /// <summary>
        /// 获取类型的主键属性列表。
        /// 识别优先级：
        ///   1. [SugarColumn(IsPrimaryKey = true)]（推荐，SqlSugar 同一特性两用）；
        ///   2. [CachePrimaryKey]（与 SqlSugar 特性二选一）；
        ///   3. 都没有 → 回退到名为 ID / Id 的属性（惯例）。
        /// 结果为空列表时 Add 会被拒绝（无主键实体不可用）。
        /// </summary>
        /// <param name="type">实体类型</param>
        public static List<PropertyInfo> GetPrimaryKeys(Type type)
        {
            return PrimaryKeys.GetOrAdd(type, t =>
            {
                var list = new List<PropertyInfo>();
                foreach (PropertyInfo p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    SugarColumn sugar = p.GetCustomAttribute<SugarColumn>();
                    if (sugar != null && sugar.IsPrimaryKey) { list.Add(p); continue; }
                    if (p.GetCustomAttribute<CachePrimaryKeyAttribute>() != null) list.Add(p);
                }
                if (list.Count == 0)
                {
                    // 惯例回退：ID / Id（旧版模型的常见命名）
                    PropertyInfo id = t.GetProperty("ID") ?? t.GetProperty("Id");
                    if (id != null) list.Add(id);
                }
                return list;
            });
        }

        /// <summary>
        /// 获取类型的缓存行为配置（[CacheBehavior] 特性）。
        /// 未标注时返回默认实例：LoadOnStartup=true / SyncDeleteToDatabase=true /
        /// EvictExpired=false / ExpiredSeconds=86400。
        /// </summary>
        public static CacheBehaviorAttribute GetBehavior(Type type)
        {
            return Behaviors.GetOrAdd(type, t => t.GetCustomAttribute<CacheBehaviorAttribute>() ?? new CacheBehaviorAttribute());
        }

        /// <summary>
        /// 取主键值字符串（复合主键用 | 连接），用于 Manager 的主键索引。
        /// 首次调用时用 Expression 编译为委托缓存，此后热路径零反射。
        /// 无主键类型返回空字符串（调用方按"无主键"拒绝处理）。
        /// </summary>
        /// <param name="obj">缓存对象</param>
        public static string GetPrimaryKeyValue(CacheObject obj)
        {
            if (obj == null) return string.Empty;
            Func<CacheObject, string> getter = PkValueGetters.GetOrAdd(obj.GetType(), BuildPkGetter);
            return getter(obj);
        }

        /// <summary>
        /// 构造主键值取值委托：o =&gt; string.Join("|", Convert.ToString(((T)o).PK1), ...)。
        /// 说明：
        ///   - Convert.ToString(object) 对 null 返回 null，string.Join 把 null 渲染为空串，安全；
        ///   - 编译一次（类型首次使用），之后调用成本 ≈ 手写代码。
        /// </summary>
        /// <param name="type">实体具体类型</param>
        private static Func<CacheObject, string> BuildPkGetter(Type type)
        {
            List<PropertyInfo> primaryKeys = GetPrimaryKeys(type);
            if (primaryKeys.Count == 0) return o => string.Empty;

            MethodInfo toStringMethod = typeof(Convert).GetMethod("ToString", new[] { typeof(object) });
            MethodInfo joinMethod = typeof(string).GetMethod("Join", new[] { typeof(string), typeof(object[]) });

            // (CacheObject o) => string.Join("|", Convert.ToString(((T)o).PK1), ...)
            ParameterExpression objParam = Expression.Parameter(typeof(CacheObject), "o");
            Expression typed = Expression.Convert(objParam, type);

            Expression[] parts = primaryKeys
                .Select(p => (Expression)Expression.Call(toStringMethod,
                    Expression.Convert(Expression.Property(typed, p), typeof(object))))
                .ToArray();

            NewArrayExpression array = Expression.NewArrayInit(typeof(object), parts);
            Expression body = Expression.Call(joinMethod, Expression.Constant("|"), array);
            return Expression.Lambda<Func<CacheObject, string>>(body, objParam).Compile();
        }
    }
}
