// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package log implements leveling and teeing on top of Go's standard
// logs package. As with the standard log package, this package
// defines a standard logger available as a package global and via
// package functions.
package log

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Level defines the level of logging. Higher levels are more
// verbose.
type Level int

const (
	// OffLevel turns logging off.
	OffLevel Level = iota
	// ErrorLevel outputs only error messages.
	ErrorLevel
	// InfoLevel is the standard error level.
	InfoLevel
	// DebugLevel outputs detailed debugging output.
	DebugLevel
)

func (l Level) String() string {
	switch l {
	default:
		return "unknown"
	case OffLevel:
		return "OFF"
	case ErrorLevel:
		return "ERROR"
	case InfoLevel:
		return "INFO"
	case DebugLevel:
		return "DEBUG"
	}
}

func LevelFromString(level string) Level {
	switch strings.ToLower(level) {
	default:
		panic(fmt.Sprintf("invalid level %s", level))
	case "off":
		return OffLevel
	case "error":
		return ErrorLevel
	case "info":
		return InfoLevel
	case "debug":
		return DebugLevel
	}
}

// An Outputter receives published log messages. Go's
// *log.Logger implements Outputter.
type Outputter interface {
	Output(calldepth int, s string) error
}

type multiOutputter []Outputter

func (m multiOutputter) Output(calldepth int, s string) error {
	var err error
	for _, out := range m {
		err1 := out.Output(calldepth, s)
		if err1 != nil {
			err = err1
		}
	}
	return err
}

// MultiOutputter returns an Outputter that outputs each
// message to all the provided outputters.
func MultiOutputter(outputters ...Outputter) Outputter {
	return multiOutputter(outputters)
}

// A Logger receives log messages at multiple levels, and publishes
// those messages to its outputter if the level (or logger) is
// active. Nil Loggers ignore all log messages.
type Logger struct {
	// Outputter receives all log messages at or below the Logger's
	// current level.
	Outputter
	// Level defines the publishing level of this Logger.
	Level Level

	Parent   *Logger
	prefix   string
	addLevel bool
}

// New creates a new Logger that publishes messsages at or below the
// provided level to the provided outputter.
func New(out Outputter, level Level) *Logger {
	if level == OffLevel {
		return nil
	}
	return &Logger{
		Outputter: out,
		Level:     level,
	}
}

// NewWithLevelPrefix creates a new Logger that prefixes each log with the logging level it was output with. Logs
// at all levels are output.
func NewWithLevelPrefix(out Outputter) *Logger {
	return &Logger{
		Outputter: out,
		Level:     DebugLevel,
		addLevel:  true,
	}
}

// Print formats a message in the manner of fmt.Print and publishes
// it to the logger at InfoLevel.
func (l *Logger) Print(v ...interface{}) {
	l.print(2, InfoLevel, "", v...)
}

// Printf formats a message in the manner of fmt.Printf and publishes
// it to the logger at InfoLevel.
func (l *Logger) Printf(format string, args ...interface{}) {
	l.printf(2, InfoLevel, "", format, args...)
}

// Error formats a message in the manner of fmt.Print and publishes
// it to the logger at ErrorLevel.
func (l *Logger) Error(v ...interface{}) {
	l.print(2, ErrorLevel, "", v...)
}

// Errorf formats a message in the manner of fmt.Printf and publishes
// it to the logger at ErrorLevel.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.printf(2, ErrorLevel, "", format, args...)
}

// Debug formats a message in the manner of fmt.Print and publishes
// it to the logger at DebugLevel.
func (l *Logger) Debug(v ...interface{}) {
	l.print(2, DebugLevel, "", v...)
}

// Debugf formats a message in the manner of fmt.Printf and publishes
// it to the logger at DebugLevel.
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.printf(2, DebugLevel, "", format, args...)
}

// At tells whether the logger is at or below the provided level.
func (l *Logger) At(level Level) bool {
	return l != nil && level <= l.Level
}

func (l *Logger) print(calldepth int, level Level, prefix string, v ...interface{}) {
	if l == nil {
		return
	}
	if l.Outputter != nil && level <= l.Level {
		_ = l.Output(calldepth+1, l.getPrefix(level, prefix)+fmt.Sprint(v...))
	}
	if l.Parent != nil {
		l.Parent.print(calldepth+1, level, l.prefix+prefix, v...)
	}
}

func (l *Logger) printf(calldepth int, level Level, prefix, format string, args ...interface{}) {
	if l == nil {
		return
	}
	if l.Outputter != nil && level <= l.Level {
		_ = l.Output(calldepth+1, l.getPrefix(level, prefix)+fmt.Sprintf(format, args...))
	}
	if l.Parent != nil {
		l.Parent.printf(calldepth+1, level, l.prefix+prefix, format, args...)
	}
}

func (l *Logger) getPrefix(level Level, prefix string) string {
	if l.addLevel {
		return fmt.Sprintf("[%s] %s", level, prefix)
	}
	return prefix
}

// Tee constructs a new logger that tees its output to the provided
// outputter and Parent logger. Messages sent to the Parent are
// prefixed with the provided prefix string. Out may be nil, in which
// cases messages are published to the Parent only.
func (l *Logger) Tee(out Outputter, prefix string) *Logger {
	if l == nil {
		return nil
	}
	return &Logger{
		Outputter: out,
		Level:     l.Level,
		Parent:    l,
		prefix:    prefix,
	}
}

// Std is the standard global logger.
// It is used by the package level logging functions.
var Std = New(log.New(os.Stderr, "", log.LstdFlags), InfoLevel)

// Print formats a message in the manner of fmt.Sprint and logs
// it to the standard logger.
func Print(v ...interface{}) {
	Std.Print(v...)
}

// Printf formats a message in the manner of fmt.Sprintf and
// logs it to the standard logger.
func Printf(format string, args ...interface{}) {
	Std.Printf(format, args...)
}

// Error formats a message in the manner of fmt.Sprint and
// logs it to the standard (error) logger.
func Error(v ...interface{}) {
	Std.Error(v...)
}

// Errorf formats a message in the manner of fmt.Sprintf and
// logs it to the standard (error) logger.
func Errorf(format string, args ...interface{}) {
	Std.Errorf(format, args...)
}

// Debug formats a message in the manner of fmt.Sprint and
// logs it to the standard (debug) logger.
func Debug(v ...interface{}) {
	Std.Debug(v...)
}

// Debugf formats a message in the manner of fmt.Sprintf and
// logs it to the standard (debug) logger.
func Debugf(format string, args ...interface{}) {
	Std.Debugf(format, args...)
}

// At returns true when the standard logger is at or above the
// provided level.
func At(level Level) bool {
	return Std.At(level)
}

// Fatal formats a message in the manner of fmt.Print, outputs it to
// the standard outputter (always), and then calls os.Exit(1).
func Fatal(v ...interface{}) {
	_ = Std.Output(2, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf formats a message in the manner of fmt.Printf, outputs it to
// the standard outputter (always), and then calls os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	_ = Std.Output(2, fmt.Sprintf(format, v...))
	os.Exit(1)
}
