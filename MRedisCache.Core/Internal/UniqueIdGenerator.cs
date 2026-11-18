using System;
using System.Threading;

// =====================================================================================
// 模块：UniqueIdGenerator / UnixTime —— ID 与时间工具
//
// 设计：63 位正数 ID = 时间戳(41bit) + 随机数(10bit) + 进程内序列(11bit)。
//   - 时间戳：ms 级，从 2020-01-01 起算，41bit 够用 69 年；
//   - 随机位：多实例之间避免碰撞，无需中心发号器、无实例注册；
//   - 序列位：Interlocked 递增，同进程同毫秒内不重复；
//   - 恒为正数（62bit 上限），兼容旧版"UID 为正 long"的约定。
//
// 相对旧版的修复：
//   - 旧版 Guid 异或后 Math.Abs 可能触发 OverflowException（long.MinValue 时）；
//   - 旧版 Random 直接共享（非线程安全）；本实现加锁保护（纳秒级，无争用压力）。
// =====================================================================================

namespace MRedisCache.Internal
{
    /// <summary>
    /// 63 位正数 ID 生成器：时间戳(41bit) + 随机数(10bit) + 进程内序列(11bit)。
    /// 多实例之间靠随机位避免碰撞，无需中心发号器；
    /// 修复旧版 Guid 异或取 Math.Abs 可能溢出的问题。
    /// </summary>
    internal static class UniqueIdGenerator
    {
        /// <summary>时间戳纪元：2020-01-01 00:00:00 UTC（ms）</summary>
        private const long EpochMs = 1577836800000L;

        private static long _sequence;                                  // 进程内序列（Interlocked 递增）
        private static readonly Random Random = new Random();          // 共享随机源
        private static readonly object RandomLock = new object();      // Random 非线程安全，加锁保护

        /// <summary>
        /// 生成全局唯一 ID（本进程视角）。
        /// 位布局：[41bit 时间戳 | 10bit 随机 | 11bit 序列]，最高位恒 0（正数）。
        /// </summary>
        public static long Next()
        {
            long timestamp = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - EpochMs) << 21;
            long sequence = Interlocked.Increment(ref _sequence) & 0x7FF;   // 11 bit 进程内序列
            long random;
            lock (RandomLock) { random = (long)Random.Next(0, 1 << 10); }  // 10 bit 随机位
            return timestamp | (random << 11) | sequence;
        }
    }

    /// <summary>
    /// Unix 时间戳工具。
    /// 统一用 UTC 秒（CacheObject 的创建/更新时间、过期判断都用它）；
    /// 注意实例间合并依赖各机器 NTP 大致同步（秒级偏差只影响合并精度）。
    /// </summary>
    internal static class UnixTime
    {
        /// <summary>当前 UTC 时间戳（秒）</summary>
        public static long NowSeconds() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }
}
