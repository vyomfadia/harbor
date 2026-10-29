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

package logger

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/jobservice/config"
	"github.com/goharbor/harbor/src/jobservice/errs"
)

func TestRetrieve(t *testing.T) {
	config.DefaultConfig.JobLoggerConfigs = []*config.LoggerConfig{
		{
			Name:  "STD_OUTPUT",
			Level: "DEBUG",
		},
		{
			Name:  "FILE",
			Level: "INFO",
			Settings: map[string]any{
				"base_dir": os.TempDir(),
			},
			Sweeper: &config.LogSweeperConfig{
				Duration: 1,
				Settings: map[string]any{
					"work_dir": os.TempDir(),
				},
			},
		},
	}

	err := Init(context.TODO())
	require.NoError(t, err)
	_, err = Retrieve("no_id")
	require.Error(t, err)

	// Missing log: not found without the "no getter" hint
	_, err = Retrieve(fakeJobID)
	require.True(t, errs.IsObjectNotFoundError(err))
	assert.NotContains(t, err.Error(), noLogDataGetterHint)

	// Existing log is returned
	logFile := path.Join(os.TempDir(), "f00000000000000000000003.log")
	require.NoError(t, os.WriteFile(logFile, []byte("job log data"), 0600))
	defer os.Remove(logFile)
	data, err := Retrieve("f00000000000000000000003")
	require.NoError(t, err)
	assert.Equal(t, "job log data", string(data))
}

func TestRetrieveStdOutputOnly(t *testing.T) {
	config.DefaultConfig.JobLoggerConfigs = []*config.LoggerConfig{
		{
			Name:  "STD_OUTPUT",
			Level: "DEBUG",
		},
	}

	err := Init(context.TODO())
	require.NoError(t, err)

	_, err = Retrieve(fakeJobID)
	require.Error(t, err)
	assert.True(t, errs.IsObjectNotFoundError(err))
	assert.Contains(t, err.Error(), fakeJobID)
	assert.Contains(t, err.Error(), noLogDataGetterHint)
}
