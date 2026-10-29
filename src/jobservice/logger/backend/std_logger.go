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

func (sl *StdOutputLogger) args(v []any) []any {
	if len(sl.prefix) == 0 {
		return v
	}
	return append([]any{sl.prefix}, v...)
}

func (sl *StdOutputLogger) format(format string) string {
	return strings.ReplaceAll(sl.prefix, "%", "%%") + format
}

// Debug ...
func (sl *StdOutputLogger) Debug(v ...any) {
	sl.backendLogger.Debug(sl.args(v)...)
}

// Debugf with format
func (sl *StdOutputLogger) Debugf(format string, v ...any) {
	sl.backendLogger.Debugf(sl.format(format), v...)
}

// Info ...
func (sl *StdOutputLogger) Info(v ...any) {
	sl.backendLogger.Info(sl.args(v)...)
}

// Infof with format
func (sl *StdOutputLogger) Infof(format string, v ...any) {
	sl.backendLogger.Infof(sl.format(format), v...)
}

// Warning ...
func (sl *StdOutputLogger) Warning(v ...any) {
	sl.backendLogger.Warning(sl.args(v)...)
}

// Warningf with format
func (sl *StdOutputLogger) Warningf(format string, v ...any) {
	sl.backendLogger.Warningf(sl.format(format), v...)
}

// Error ...
func (sl *StdOutputLogger) Error(v ...any) {
	sl.backendLogger.Error(sl.args(v)...)
}

// Errorf with format
func (sl *StdOutputLogger) Errorf(format string, v ...any) {
	sl.backendLogger.Errorf(sl.format(format), v...)
}

// Fatal error
func (sl *StdOutputLogger) Fatal(v ...any) {
	sl.backendLogger.Fatal(sl.args(v)...)
}

// Fatalf error
func (sl *StdOutputLogger) Fatalf(format string, v ...any) {
	sl.backendLogger.Fatalf(sl.format(format), v...)
}
