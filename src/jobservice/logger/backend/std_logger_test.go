package backend

import (
	"bytes"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/lib/log"
)

// Test std logger
func TestStdLogger(t *testing.T) {
	l := NewStdOutputLogger("DEBUG", StdErr, 4)
	l.Debug("TestStdLogger")
	l.Debugf("%s", "TestStdLogger")
	l.Info("TestStdLogger")
	l.Infof("%s", "TestStdLogger")
	l.Warning("TestStdLogger")
	l.Warningf("%s", "TestStdLogger")
	l.Error("TestStdLogger")
	l.Errorf("%s", "TestStdLogger")
}

// Test std logger tagged with a key
func TestStdLoggerWithKey(t *testing.T) {
	buf := &bytes.Buffer{}
	base := &StdOutputLogger{backendLogger: log.New(buf, log.NewTextFormatter(), log.DebugLevel)}
	l := base.WithKey("job%id")

	l.Info("message", 1)
	l.Warningf("formatted %d", 2)
	base.Error("untagged")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 3)
	assert.True(t, strings.HasSuffix(lines[0], "[job%id] message1"), lines[0])
	assert.True(t, strings.HasSuffix(lines[1], "[job%id] formatted 2"), lines[1])
	assert.True(t, strings.HasSuffix(lines[2], "untagged"), lines[2])
	assert.NotContains(t, lines[2], "job%id")
}

// Test every line of a multiline message is tagged, including newlines from arguments
func TestStdLoggerWithKeyMultiline(t *testing.T) {
	buf := &bytes.Buffer{}
	l := (&StdOutputLogger{backendLogger: log.New(buf, log.NewTextFormatter(), log.DebugLevel)}).WithKey("job")

	l.Infof("\n%s", "| a |\n| b |\n")
	l.Error("x\ny")

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	require.Len(t, lines, 5, buf.String())
	assert.True(t, strings.HasSuffix(lines[0], " [job] "), lines[0])
	assert.Equal(t, "[job] | a |", lines[1])
	assert.Equal(t, "[job] | b |", lines[2])
	assert.True(t, strings.HasSuffix(lines[3], " [job] x"), lines[3])
	assert.Equal(t, "[job] y", lines[4])
}

// Test the tagged logger reports the caller's file and line
func TestStdLoggerWithKeyCaller(t *testing.T) {
	buf := &bytes.Buffer{}
	l := (&StdOutputLogger{backendLogger: log.New(buf, log.NewTextFormatter(), log.DebugLevel, 4)}).WithKey("job")

	_, _, line, _ := runtime.Caller(0)
	l.Infof("a\nb")
	l.Warning("c")

	out := buf.String()
	assert.Contains(t, out, fmt.Sprintf("std_logger_test.go:%d]", line+1), out)
	assert.Contains(t, out, fmt.Sprintf("std_logger_test.go:%d]", line+2), out)
}

// Test suppressed tagged messages are neither written nor formatted
func TestStdLoggerWithKeySuppressed(t *testing.T) {
	buf := &bytes.Buffer{}
	l := (&StdOutputLogger{backendLogger: log.New(buf, log.NewTextFormatter(), log.ErrorLevel)}).WithKey("job")

	allocs := testing.AllocsPerRun(100, func() {
		l.Debugf("%s", "suppressed")
		l.Info("suppressed")
	})
	assert.Zero(t, allocs)
	assert.Zero(t, buf.Len())
}

func BenchmarkStdLoggerWithKeySuppressed(b *testing.B) {
	l := (&StdOutputLogger{backendLogger: log.New(io.Discard, log.NewTextFormatter(), log.ErrorLevel)}).WithKey("job")
	b.ReportAllocs()
	for b.Loop() {
		l.Debugf("%s", "suppressed")
	}
}
