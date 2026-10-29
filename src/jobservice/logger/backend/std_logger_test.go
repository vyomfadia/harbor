package backend

import (
	"bytes"
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
