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

	parent *Logger
	prefix string
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
		l.Output(calldepth+1, prefix+fmt.Sprint(v...))
	}
	if l.parent != nil {
		l.parent.print(calldepth+1, level, prefix+l.prefix, v...)
	}
}

func (l *Logger) printf(calldepth int, level Level, prefix, format string, args ...interface{}) {
	if l == nil {
		return
	}
	if l.Outputter != nil && level <= l.Level {
		l.Output(calldepth+1, prefix+fmt.Sprintf(format, args...))
	}
	if l.parent != nil {
		l.parent.printf(calldepth+1, level, prefix+l.prefix, format, args...)
	}
}

// Tee constructs a new logger that tees its output to the provided
// outputter and parent logger. Messages sent to the parent are
// prefixed with the provided prefix string. Out may be nil, in which
// cases messages are published to the parent only.
func (l *Logger) Tee(out Outputter, prefix string) *Logger {
	if l == nil {
		return nil
	}
	return &Logger{
		Outputter: out,
		Level:     l.Level,
		parent:    l,
		prefix:    prefix,
	}
}

// Std is the standard logger.
var Std = New(log.New(os.Stderr, "", log.LstdFlags), InfoLevel)

// The following are convenience functions to call on
// common methods on the Std logger.
var (
	Print  = Std.Print
	Printf = Std.Printf
	Error  = Std.Error
	Errorf = Std.Errorf
	Debug  = Std.Debug
	Debugf = Std.Debugf
	At     = Std.At
)

// Fatal formats a message in the manner of fmt.Print, outputs it to
// the standard outputter (always), and then calls os.Exit(1).
func Fatal(v ...interface{}) {
	Std.Output(2, fmt.Sprint(v...))
	os.Exit(1)
}

// Fatalf formats a message in the manner of fmt.Printf, outputs it to
// the standard outputter (always), and then calls os.Exit(1).
func Fatalf(format string, v ...interface{}) {
	Std.Output(2, fmt.Sprintf(format, v...))
	os.Exit(1)
}
