using System;
using System.Diagnostics;

// =====================================================================================
// 模块：Log —— 轻量日志门面（全库唯一日志 API）
//
// 设计：
//   - 单一入口（Log.Info/Warn/Error/Debug），替代旧版 Logger.Instance / getLogger() 双 API；
//   - 默认输出 Console + Trace；通过 Writer 属性一行接入业务日志框架
//     （log4net / NLog / Serilog 等）；
//   - MinLevel 运行时可调（生产建议 Info，排查问题开 Debug）；
//   - Writer 抛异常不影响主流程（日志永远不能打挂业务）。
// =====================================================================================

namespace MRedisCache
{
    /// <summary>
    /// 轻量日志门面，默认输出到 Console 与 Trace。
    /// 可通过 <see cref="Writer"/> 替换为业务日志实现（例如接入 log4net / NLog / Serilog）。
    /// 统一一个 API，避免旧版 Logger.Instance / getLogger() 双入口问题。
    /// </summary>
    public static class Log
    {
        /// <summary>日志级别（数值越大越严重，MinLevel 以下的日志被丢弃）</summary>
        public enum Level { Debug = 0, Info = 1, Warn = 2, Error = 3 }

        /// <summary>最低输出级别（默认 Info；排查问题时设为 Debug）</summary>
        public static Level MinLevel { get; set; } = Level.Info;

        /// <summary>
        /// 自定义输出；为 null 时输出到 Console + Trace。
        /// 示例：Log.Writer = line =&gt; nlogLogger.Info(line);
        /// </summary>
        public static Action<string> Writer = null;

        /// <summary>调试日志（详细流转信息，默认关闭）</summary>
        public static void Debug(string message) => Write(Level.Debug, message);

        /// <summary>常规信息（启动/装配/关闭等生命周期事件）</summary>
        public static void Info(string message) => Write(Level.Info, message);

        /// <summary>告警（可自愈的异常路径：重试、强关、竞态兜底等）</summary>
        public static void Warn(string message) => Write(Level.Warn, message);

        /// <summary>错误（含异常对象的简明格式）</summary>
        public static void Error(string message) => Write(Level.Error, message);

        /// <summary>错误（带异常类型与消息；不打印堆栈以控制日志量，需要堆栈时用 Debug 级场景复现）</summary>
        public static void Error(string message, Exception ex) => Write(Level.Error, ex == null ? message : $"{message} :: {ex.GetType().Name}: {ex.Message}");

        /// <summary>
        /// 统一输出：级别过滤 → 格式化（时间/来源/级别）→ 分发（Writer 或 Console+Trace）。
        /// Writer 自身异常被吞掉 —— 日志失败不能影响主流程。
        /// </summary>
        private static void Write(Level level, string message)
        {
            if (level < MinLevel) return;
            string line = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}][MRedisCache][{level}] {message}";
            if (Writer != null)
            {
                try { Writer(line); } catch { /* 日志失败不能影响主流程 */ }
                return;
            }
            Console.WriteLine(line);
            Trace.WriteLine(line);
        }
    }
}
