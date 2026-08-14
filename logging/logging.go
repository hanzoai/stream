package logging

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

// logging levels
const (
	TRACE = "TRACE"
	DEBUG = "DEBUG"
	INFO  = "INFO"
	WARN  = "WARN"
	ERROR = "ERROR"
)

// severity orders the levels. TRACE was missing from it, so it scored zero by
// the absence of an entry and the order came out right by accident; saying it
// is cheaper than rediscovering why it works.
var severity = map[string]int{
	TRACE: 0,
	DEBUG: 1,
	INFO:  2,
	WARN:  3,
	ERROR: 4,
}

// threshold is the level at or above which a line is written. Every goroutine
// that logs reads it and SetLogLevel writes it, so it is held atomically — a
// host that changes level while connections are live raced every log call.
var threshold atomic.Value

// Lines go to stdout, chosen once. This used to be a log.SetOutput on EVERY
// line, so every connection goroutine wrote a process-global concurrently and
// no caller could read back what was written. Choosing it here says the same
// thing once, and a test can point the logger somewhere it can read.
func init() {
	threshold.Store(INFO)
	log.SetOutput(os.Stdout)
}

// SetLogLevel sets the log level for filtering logs
func SetLogLevel(logLevel string) {
	threshold.Store(logLevel)
}

// LogLevel reports the level at or above which lines are written.
func LogLevel() string {
	return threshold.Load().(string)
}

// Log writes a log message at a specified level, formatted with optional arguments
func Log(level, message string, a ...any) {
	if severity[level] >= severity[LogLevel()] {
		log.Printf("[%s] %s\n", level, fmt.Sprintf(message, a...))
	}
}

// Trace logs a message at TRACE level
func Trace(message string, a ...any) {
	Log(TRACE, message, a...)
}

// Debug logs a message at DEBUG level
func Debug(message string, a ...any) {
	Log(DEBUG, message, a...)
}

// Info logs a message at INFO level
func Info(message string, a ...any) {
	Log(INFO, message, a...)
}

// Warn logs a message at WARN level
func Warn(message string, a ...any) {
	Log(WARN, message, a...)
}

// Error logs a message at ERROR level
func Error(message string, a ...any) {
	Log(ERROR, message, a...)
}

// Panic exists with a panic
func Panic(message string, a ...any) {
	panic(fmt.Sprintf(message, a...))
}
