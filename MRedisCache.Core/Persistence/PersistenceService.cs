using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using SqlSugar;

// =====================================================================================
// 模块：PersistenceService —— 数据库持久化服务（SqlSugar ORM，L3 层核心）
//
// 职责：
//   1. 落库线程（write-behind）：周期遍历全部表，把脏队列中的对象交给
//      CacheManager.FlushDirty 按 Insert/Update/Delete 分类批量写入数据库；
//   2. 查询接口：QueryAll（Manager 启动全量加载）、QueryByPrimaryKeys（读穿透回源）；
//   3. 过期淘汰：同周期低优先级执行（限流），不挤占落库；
//   4. 优雅关闭：停止线程后执行最后一轮落库，尽量不丢数据。
//
// SqlSugar 使用要点：
//   - 每轮批量操作创建独立的 SqlSugarClient（IsAutoCloseConnection），
//     避免多线程共享状态（替代旧版每操作 new DBMssql 的手写 SQL 模式）；
//   - 实体映射靠 [SugarTable] / [SugarColumn] 特性（InitKeyType.Attribute），
//     彻底移除旧版的 SQL 拼接与 SourceGenerator 代码生成；
//   - Insertable/Updateable/Deleteable 批量接口天然按主键定位。
//
// 相对旧版 DatabaseSyncManager 的关键修复：
//   1. 成功之后才清脏标志（旧版先清后写，批量失败即丢数据，恢复代码被注释）；
//   2. 删除失败无处理（TODO 遗留）→ 保持 IsDeleted 隐藏态持续重试；
//   3. 每条脏数据 O(1) 精确定位（脏队列），不再全表扫描找 IsDirty。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// 数据库持久化服务（SqlSugar ORM）：
    ///  1. 落库线程周期性把各表脏队列中的数据按 Insert/Update/Delete 分类批量写入；
    ///  2. 成功后才清除脏标志；失败保留脏标志并重新入队，下个周期重试（修复旧版丢数据问题）；
    ///  3. 关闭时执行最后一轮落库，尽量不丢数据；
    ///  4. 同一周期顺带执行过期淘汰（限流）。
    /// </summary>
    internal sealed class PersistenceService : IDisposable
    {
        private readonly MRedisCacheClient _client;   // 所属缓存客户端
        private readonly MRedisCacheOptions _options; // 配置（连接信息 / 批量参数）
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _worker;              // 落库线程任务
        private int _disposed;                      // Dispose 幂等守卫

        /// <summary>
        /// 构造并启动落库线程（间隔 FlushIntervalMs，批量 FlushBatchSize）。
        /// </summary>
        public PersistenceService(MRedisCacheClient client, MRedisCacheOptions options)
        {
            _client = client;
            _options = options;
            _worker = Task.Run(() => RunAsync(_cts.Token));
        }

        /// <summary>
        /// 创建独立的 SqlSugarClient。
        /// 每次批量操作创建新实例 + IsAutoCloseConnection：
        /// 天然线程安全（不共享上下文），连接用完即还。
        /// </summary>
        internal SqlSugarClient CreateClient()
        {
            return new SqlSugarClient(new ConnectionConfig
            {
                ConnectionString = _options.ConnectionString,
                DbType = _options.DbType,
                IsAutoCloseConnection = true,
                InitKeyType = InitKeyType.Attribute   // 特性映射：[SugarTable]/[SugarColumn]
            });
        }

        /// <summary>
        /// 落库线程主循环：FlushOnce（落库）→ EvictOnce（淘汰）→ 延迟 → 循环。
        /// 单周期异常只记日志不退出（线程永生，直到 Dispose）。
        /// </summary>
        private async Task RunAsync(CancellationToken token)
        {
            Log.Info($"落库线程已启动（间隔 {_options.FlushIntervalMs}ms，批量 {_options.FlushBatchSize}）。");
            while (!token.IsCancellationRequested)
            {
                try
                {
                    FlushOnce();
                    EvictOnce();
                }
                catch (Exception ex)
                {
                    // 防御性兜底：FlushDirty 内部已分类捕获，这里只防意外错误
                    Log.Error("落库周期执行异常", ex);
                }
                try { await Task.Delay(_options.FlushIntervalMs, token); }
                catch (OperationCanceledException) { break; }
            }
            Log.Info("落库线程退出。");
        }

        /// <summary>
        /// 落库一轮：遍历全部已装配 Manager 的表，逐表批量落库。
        /// （Manager 为 null 的表 = 已建存储但类型未装配，跳过）
        /// </summary>
        private void FlushOnce()
        {
            foreach (TableStore table in _client.Store.Tables)
            {
                ICacheManager manager = table.Manager;
                if (manager == null) continue;
                manager.FlushDirty();
            }
        }

        /// <summary>
        /// 过期淘汰一轮：全局预算 EvictBatchLimit，按表消耗（淘汰量从预算中扣除），
        /// 预算用尽即停止 —— 淘汰优先级低于落库，避免挤占数据库写入。
        /// </summary>
        private void EvictOnce()
        {
            int budget = _options.EvictBatchLimit;
            foreach (TableStore table in _client.Store.Tables)
            {
                if (budget <= 0) break;
                ICacheManager manager = table.Manager;
                if (manager == null || !table.EvictExpired) continue;
                budget -= manager.ProcessExpired(budget);
            }
        }

        #region 查询接口（Manager 读穿透 / 启动加载使用）

        /// <summary>
        /// 全表查询（Manager.LoadFromDatabase 启动加载用）。
        /// SELECT 由 SqlSugar 按特性生成，等价旧版 SELECT * FROM {表名} 但无注入风险。
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <returns>全部行；查询异常向上抛出（由调用方决定降级策略）</returns>
        public List<T> QueryAll<T>() where T : class, new()
        {
            using (SqlSugarClient db = CreateClient())
            {
                return db.Queryable<T>().ToList();
            }
        }

        /// <summary>
        /// 按主键回源查询（Manager.Get(probe) 读穿透用）。
        /// 用 Linq.Expression 按缓存的主键 PropertyInfo 动态构造等值条件
        /// （x =&gt; x.PK1 == v1 &amp;&amp; x.PK2 == v2 ...），SqlSugar 翻译成参数化 SQL。
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="probe">主键探测对象（只需主键字段有值）</param>
        /// <param name="primaryKeys">主键属性列表（EntityMeta 缓存）</param>
        /// <returns>首条匹配行；无匹配返回 null</returns>
        public T QueryByPrimaryKeys<T>(T probe, List<PropertyInfo> primaryKeys) where T : class, new()
        {
            using (SqlSugarClient db = CreateClient())
            {
                ISugarQueryable<T> query = db.Queryable<T>();
                foreach (PropertyInfo pk in primaryKeys)
                {
                    // 逐个主键构造 x => x.{pk} == {probe 的 pk 值}（类型保持，避免隐式转换）
                    ParameterExpression param = Expression.Parameter(typeof(T), "x");
                    BinaryExpression body = Expression.Equal(
                        Expression.Property(param, pk),
                        Expression.Constant(pk.GetValue(probe), pk.PropertyType));
                    query = query.Where(Expression.Lambda<Func<T, bool>>(body, param));
                }
                List<T> rows = query.ToList();
                return rows.Count > 0 ? rows[0] : null;
            }
        }

        #endregion

        /// <summary>
        /// 批量执行增删改（CacheManager.FlushUids 调用）：
        /// 同一轮内按类型分三条批量语句（Insertable / Updateable / Deleteable）。
        /// Updateable 按主键定位、整行更新（非字段级 —— 见架构文档 6.2 已知限制）。
        /// 任一语句失败整批抛异常（由 FlushUids 统一走重试路径）。
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="inserts">新插入对象（IsNew）</param>
        /// <param name="updates">待更新对象（IsDirty）</param>
        /// <param name="deletes">待删除对象（IsDeleted）</param>
        public void Execute<T>(List<T> inserts, List<T> updates, List<T> deletes) where T : class, new()
        {
            using (SqlSugarClient db = CreateClient())
            {
                if (inserts.Count > 0) db.Insertable(inserts).ExecuteCommand();
                if (updates.Count > 0) db.Updateable(updates).ExecuteCommand();
                if (deletes.Count > 0) db.Deleteable(deletes).ExecuteCommand();
            }
        }

        /// <summary>
        /// 关闭服务（幂等）：取消令牌 → 等待落库线程退出（10 秒超时强关）
        /// → 执行最后一轮落库（尽量不丢数据；本轮失败只记日志）。
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
            try
            {
                _cts.Cancel();
                if (!_worker.Wait(TimeSpan.FromSeconds(10)))
                    Log.Warn("落库线程 10 秒内未退出，强制关闭。");
                // 关闭前把剩余脏数据尽量落最后一轮
                try { FlushOnce(); }
                catch (Exception ex) { Log.Error("关闭前最终落库失败", ex); }
            }
            catch (Exception ex)
            {
                Log.Error("关闭 PersistenceService 异常", ex);
            }
        }
    }
}
