// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"
	"os"
	"strings"

	"github.com/goharbor/harbor/src/lib/log"
)

const (
	// StdOut represents os.Stdout
	StdOut = "std_out"
	// StdErr represents os.StdErr
	StdErr = "std_err"
)

// StdOutputLogger is an implementation of logger.Interface.
// It outputs the log to the stdout/stderr.
type StdOutputLogger struct {
	backendLogger *log.Logger
	// prefix is prepended to every log message
	prefix string
}

// NewStdOutputLogger creates a new std output logger
func NewStdOutputLogger(level string, output string, depth int) *StdOutputLogger {
	logLevel := parseLevel(level)
	logStream := os.Stdout
	if output == StdErr {
		logStream = os.Stderr
	}
	backendLogger := log.New(logStream, log.NewTextFormatter(), logLevel, depth)

	return &StdOutputLogger{
		backendLogger: backendLogger,
	}
}

// WithKey returns a copy of the logger which prefixes every message with the key
func (sl *StdOutputLogger) WithKey(key string) *StdOutputLogger {
	return &StdOutputLogger{
		backendLogger: sl.backendLogger,
		prefix:        "[" + key + "] ",
	}
}

// tag prefixes every line of msg. A trailing newline is left alone since the
// formatter only adds one when missing.
func (sl *StdOutputLogger) tag(msg string) string {
	body, nl := strings.CutSuffix(msg, "\n")
	body = sl.prefix + strings.ReplaceAll(body, "\n", "\n"+sl.prefix)
	if nl {
		body += "\n"
	}
	return body
}

// enabled reports whether a message at lvl will be written, so suppressed
// messages are not formatted.
func (sl *StdOutputLogger) enabled(lvl log.Level) bool {
	return sl.backendLogger.GetLevel() <= lvl
}

// Each method calls the backend directly to keep the configured caller depth.

// Debug ...
func (sl *StdOutputLogger) Debug(v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Debug(v...)
	} else if sl.enabled(log.DebugLevel) {
		sl.backendLogger.Debug(sl.tag(fmt.Sprint(v...)))
	}
}

// Debugf with format
func (sl *StdOutputLogger) Debugf(format string, v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Debugf(format, v...)
	} else if sl.enabled(log.DebugLevel) {
		sl.backendLogger.Debug(sl.tag(fmt.Sprintf(format, v...)))
	}
}

// Info ...
func (sl *StdOutputLogger) Info(v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Info(v...)
	} else if sl.enabled(log.InfoLevel) {
		sl.backendLogger.Info(sl.tag(fmt.Sprint(v...)))
	}
}

// Infof with format
func (sl *StdOutputLogger) Infof(format string, v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Infof(format, v...)
	} else if sl.enabled(log.InfoLevel) {
		sl.backendLogger.Info(sl.tag(fmt.Sprintf(format, v...)))
	}
}

// Warning ...
func (sl *StdOutputLogger) Warning(v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Warning(v...)
	} else if sl.enabled(log.WarningLevel) {
		sl.backendLogger.Warning(sl.tag(fmt.Sprint(v...)))
	}
}

// Warningf with format
func (sl *StdOutputLogger) Warningf(format string, v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Warningf(format, v...)
	} else if sl.enabled(log.WarningLevel) {
		sl.backendLogger.Warning(sl.tag(fmt.Sprintf(format, v...)))
	}
}

// Error ...
func (sl *StdOutputLogger) Error(v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Error(v...)
	} else if sl.enabled(log.ErrorLevel) {
		sl.backendLogger.Error(sl.tag(fmt.Sprint(v...)))
	}
}

// Errorf with format
func (sl *StdOutputLogger) Errorf(format string, v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Errorf(format, v...)
	} else if sl.enabled(log.ErrorLevel) {
		sl.backendLogger.Error(sl.tag(fmt.Sprintf(format, v...)))
	}
}

// Fatal error
func (sl *StdOutputLogger) Fatal(v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Fatal(v...)
	} else {
		sl.backendLogger.Fatal(sl.tag(fmt.Sprint(v...)))
	}
}

// Fatalf error
func (sl *StdOutputLogger) Fatalf(format string, v ...any) {
	if len(sl.prefix) == 0 {
		sl.backendLogger.Fatalf(format, v...)
	} else {
		sl.backendLogger.Fatal(sl.tag(fmt.Sprintf(format, v...)))
	}
}
