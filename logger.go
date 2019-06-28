package kinsumer

import "log"

type Logger interface {
	Log(string, ...interface{})
}

type DefaultLogger struct{}

func (*DefaultLogger) Log(format string, v ...interface{}) {
	log.Printf(format, v)
}
