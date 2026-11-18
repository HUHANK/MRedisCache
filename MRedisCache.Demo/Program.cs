using System;
using System.Collections.Concurrent;
using MRedisCache;
using SqlSugar;

// =====================================================================================
// 模块：MRedisCache.Demo —— 控制台示例（CRUD / 自定义二级索引 / 生命周期全流程演示）
//
// 默认以「纯内存模式」运行（无需 Redis / 数据库即可跑通全部断言输出）；
// 启用数据库 / Redis 见 Main 中的注释配置。
// =====================================================================================

namespace MRedisCache.Demo
{
    /// <summary>
    /// 示例实体：本地内存 + Redis + 数据库（write-behind）三级缓存。
    /// 要点：
    ///   1. 继承 CacheRedisDatabaseObject 决定层级（另有 CacheLocalObject / CacheRedisObject）；
    ///   2. 属性 setter 必须走 SetProperty，变更才会自动进入
    ///      「自定义索引维护 → Redis 异步写 → 脏数据落库」三个通道；
    ///   3. [SugarTable] / [SugarColumn] 是 SqlSugar 的数据库映射（表名/主键/列）；
    ///      [CacheBehavior] 是缓存行为（启动加载 / 删除同步数据库）。
    /// </summary>
    [SugarTable("DEMO_STATION")]
    [CacheBehavior(LoadOnStartup = true, SyncDeleteToDatabase = true)]
    public class DemoStation : CacheRedisDatabaseObject
    {
        private string _stationId;
        private string _stationName;
        private bool _enabled;

        /// <summary>主键（SugarColumn 主键特性同时被缓存层识别为主键索引键）</summary>
        [SugarColumn(IsPrimaryKey = true, Length = 64)]
        public string STATION_ID
        {
            get { return _stationId; }
            set { SetProperty(ref _stationId, value); }
        }

        [SugarColumn(Length = 128, IsNullable = true)]
        public string STATION_NAME
        {
            get { return _stationName; }
            set { SetProperty(ref _stationName, value); }
        }

        [SugarColumn(IsNullable = true)]
        public bool ENABLED
        {
            get { return _enabled; }
            set { SetProperty(ref _enabled, value); }
        }
    }

    /// <summary>
    /// 示例 Manager：演示自定义二级索引（按名称查询）。
    /// 索引的三个人口（Add/Remove/Update）由框架在本地与远端变更时自动调用：
    ///   - AddCustomIndex：对象进入缓存（Add / 回源 / 远端新增）；
    ///   - RemoveCustomIndex：对象移出缓存（Remove / 过期 / 远端删除）；
    ///   - UpdateCustomIndex：字段变化（本地 SetProperty 与远端 Populate 都会触发，
    ///     oldValue 用于索引键迁移 —— 改名后旧键自动清理、新键自动建立）。
    /// </summary>
    public sealed class DemoStationMgr : CacheManager<DemoStation>
    {
        /// <summary>二级索引：STATION_NAME → UID（O(1) 按名查询）</summary>
        private readonly ConcurrentDictionary<string, long> _nameIndex = new ConcurrentDictionary<string, long>();

        /// <summary>构造即装配（从 Redis / 数据库加载），同一类型全进程只允许一次。</summary>
        public DemoStationMgr(MRedisCacheClient client) : base(client) { }

        protected override void AddCustomIndex(DemoStation value)
        {
            if (!string.IsNullOrEmpty(value.STATION_NAME))
                _nameIndex[value.STATION_NAME] = value.CacheObjectUID;
        }

        protected override void RemoveCustomIndex(DemoStation value)
        {
            if (!string.IsNullOrEmpty(value.STATION_NAME))
                _nameIndex.TryRemove(value.STATION_NAME, out _);
        }

        protected override void UpdateCustomIndex(DemoStation value, string fieldName, object oldValue)
        {
            // 只关心名称字段的变化（oldValue = 改名前的旧名称）
            if (fieldName != nameof(DemoStation.STATION_NAME)) return;
            string oldKey = oldValue as string;
            if (!string.IsNullOrEmpty(oldKey)) _nameIndex.TryRemove(oldKey, out _);
            if (!string.IsNullOrEmpty(value.STATION_NAME))
                _nameIndex[value.STATION_NAME] = value.CacheObjectUID;
        }

        /// <summary>按名称 O(1) 查询（走二级索引 → Get(uid)）</summary>
        public DemoStation GetByName(string name)
        {
            return _nameIndex.TryGetValue(name, out long uid) ? Get(uid) : null;
        }
    }

    /// <summary>演示入口：完整生命周期 Create → Manager → CompleteInitialLoad → CRUD → Dispose。</summary>
    internal static class Program
    {
        private static void Main()
        {
            var options = new MRedisCacheOptions
            {
                CacheName = "Demo",
                EnableDatabase = false,
                // 启用数据库（SqlSugar）：
                // EnableDatabase = true,
                // DbType = DbType.SqlServer,
                // ConnectionString = "Server=.;Database=Demo;User Id=sa;Password=***;",
                // 启用 Redis（多实例同步）：
                // RedisEndpoints = new List<string> { "127.0.0.1:6379" }
            };

            using (var client = MRedisCacheClient.Create(options))
            {
                // 1. 创建 Manager（构造即完成 Redis / 数据库初始加载；
                //    也可用 client.GetManager<DemoStation>()，但那样拿不到子类的自定义索引）
                var stations = new DemoStationMgr(client);

                // 2. 全部 Manager 创建完成后调用，远端变更开始实时应用
                //   （此前收到的远端消息会被缓冲，防止覆盖正在加载的数据）
                client.CompleteInitialLoad();

                // 3. 新增：分配 UID → 入内存/主键索引/自定义索引 → 入脏队列 → 投递 Redis
                var station = new DemoStation { STATION_ID = "ST01", STATION_NAME = "装载站", ENABLED = true };
                stations.Add(station);
                Console.WriteLine($"新增后数量: {stations.Count}");

                // 4. 主键查询 / 二级索引查询（均 O(1)，读路径实测 ~30ns）
                DemoStation byId = stations.GetByPrimaryKey("ST01");
                DemoStation byName = stations.GetByName("装载站");
                Console.WriteLine($"主键查询: {byId?.STATION_ID} - {byId?.STATION_NAME}");
                Console.WriteLine($"索引查询: {byName?.STATION_ID} - {byName?.STATION_NAME}");

                // 5. 属性修改：SetProperty 自动触发「索引维护 → Redis 异步写 → 脏数据落库」
                //   （注意改名后旧索引键被清理、新索引键自动建立 —— 下方查询可验证）
                byId.STATION_NAME = "卸载站";
                DemoStation renamed = stations.GetByName("卸载站");
                Console.WriteLine($"改名后索引查询: {renamed?.STATION_ID} - {renamed?.STATION_NAME}");

                // 6. 删除（RedisDatabase 层级：先删缓存，数据库由落库线程批量删除）
                stations.Remove(byId);
                Console.WriteLine($"删除后数量: {stations.Count}");
            }
            // 7. Dispose：先停落库线程（含最后一轮落库），再关 Redis（各 10 秒超时强关）

            Console.WriteLine("Demo 结束。");
        }
    }
}
