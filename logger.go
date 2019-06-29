package kinsumer

import "log"

// Logger is a minimal interface to allow custom loggers to be used
type Logger interface {
	Log(string, ...interface{})
}

// DefaultLogger is a logger that will log using the
// standard golang log library
type DefaultLogger struct{}

// Log implementation that uses golang log library
func (*DefaultLogger) Log(format string, v ...interface{}) {
	log.Printf(format, v...)
}
