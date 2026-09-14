package rediselection

// Logger 是库的最小日志接口，不绑定任何具体日志实现，
// 避免向使用方传染日志依赖。默认为静默丢弃，
// 通过 WithLogger 注入适配，例如：
//
//	WithLogger(log.New(os.Stderr, "", log.LstdFlags))
//
// 或适配 zap/logrus：
//
//	WithLogger(stdlog.New(log.New(os.Stderr, "", 0), "rediselection: ", 0))
type Logger interface {
	Printf(format string, v ...interface{})
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...interface{}) {}
