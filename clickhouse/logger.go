package clickhouse

var loggerProgress LoggerProgress = nil

type LoggerProgress interface {
	Progress(in Progress)
}
