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

package dao

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"github.com/goharbor/harbor/src/common/dao"
	"github.com/goharbor/harbor/src/jobservice/job"
	"github.com/goharbor/harbor/src/lib/cache"
	_ "github.com/goharbor/harbor/src/lib/cache/redis"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/q"
	libredis "github.com/goharbor/harbor/src/lib/redis"
	"github.com/stretchr/testify/suite"
)

type executionDAOTestSuite struct {
	suite.Suite
	ctx          context.Context
	executionDAO *executionDAO
	taskDao      *taskDAO
	executionID  int64
}

func (e *executionDAOTestSuite) SetupSuite() {
	dao.PrepareTestForPostgresSQL()
	e.ctx = orm.Context()
	e.taskDao = &taskDAO{}
	e.executionDAO = &executionDAO{
		taskDAO: e.taskDao,
	}
	// initializes cache for testing
	err := cache.Initialize(cache.Redis, "redis://localhost:6379/0")
	e.NoError(err)
}

func (e *executionDAOTestSuite) SetupTest() {
	id, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType: "test",
		Trigger:    "test",
		ExtraAttrs: `{"key":"value"}`,
	})
	e.Require().Nil(err)
	e.executionID = id
}

func (e *executionDAOTestSuite) TearDownTest() {
	err := e.executionDAO.Delete(e.ctx, e.executionID)
	e.Nil(err)
}

func (e *executionDAOTestSuite) TestCount() {
	count, err := e.executionDAO.Count(e.ctx, &q.Query{
		Keywords: map[string]any{
			"VendorType":     "test",
			"ExtraAttrs.key": "value",
		},
	})
	e.Require().Nil(err)
	e.Equal(int64(1), count)

	count, err = e.executionDAO.Count(e.ctx, &q.Query{
		Keywords: map[string]any{
			"VendorType":     "test",
			"ExtraAttrs.key": "incorrect-value",
		},
	})
	e.Require().Nil(err)
	e.Equal(int64(0), count)
}

func (e *executionDAOTestSuite) TestList() {
	executions, err := e.executionDAO.List(e.ctx, &q.Query{
		Keywords: map[string]any{
			"VendorType":     "test",
			"ExtraAttrs.key": "value",
		},
	})
	e.Require().Nil(err)
	e.Require().Len(executions, 1)
	e.Equal(e.executionID, executions[0].ID)

	executions, err = e.executionDAO.List(e.ctx, &q.Query{
		Keywords: map[string]any{
			"VendorType":     "test",
			"ExtraAttrs.key": "incorrect-value",
		},
	})
	e.Require().Nil(err)
	e.Require().Len(executions, 0)
}

func (e *executionDAOTestSuite) TestGet() {
	// not exist
	_, err := e.executionDAO.Get(e.ctx, 10000)
	e.Require().NotNil(err)
	e.True(errors.IsNotFoundErr(err))

	// exist
	execution, err := e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.NotNil(execution)
}

func (e *executionDAOTestSuite) TestCreate() {
	// happy pass is covered by SetupTest
}

func (e *executionDAOTestSuite) TestUpdate() {
	// not exist
	err := e.executionDAO.Update(e.ctx, &Execution{ID: 10000}, "Status")
	e.Require().NotNil(err)
	e.True(errors.IsNotFoundErr(err))

	// exist
	err = e.executionDAO.Update(e.ctx, &Execution{
		ID:     e.executionID,
		Status: "failed",
	}, "Status")
	e.Require().Nil(err)
	execution, err := e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal("failed", execution.Status)
}

func (e *executionDAOTestSuite) TestDelete() {
	// not exist
	err := e.executionDAO.Delete(e.ctx, 10000)
	e.Require().NotNil(err)
	e.True(errors.IsNotFoundErr(err))

	// happy pass is covered by TearDownTest
}

func (e *executionDAOTestSuite) TestGetMetrics() {
	taskID01, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.SuccessStatus.String(),
		StatusCode:  job.SuccessStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID01)

	taskID02, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.StoppedStatus.String(),
		StatusCode:  job.StoppedStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID02)

	taskID03, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.ErrorStatus.String(),
		StatusCode:  job.ErrorStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID03)

	taskID04, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.PendingStatus.String(),
		StatusCode:  job.PendingStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID04)

	taskID05, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.RunningStatus.String(),
		StatusCode:  job.RunningStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID05)

	taskID06, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.ScheduledStatus.String(),
		StatusCode:  job.ScheduledStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID06)

	metrics, err := e.executionDAO.GetMetrics(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(int64(6), metrics.TaskCount)
	e.Equal(int64(1), metrics.SuccessTaskCount)
	e.Equal(int64(1), metrics.StoppedTaskCount)
	e.Equal(int64(1), metrics.ErrorTaskCount)
	e.Equal(int64(1), metrics.PendingTaskCount)
	e.Equal(int64(1), metrics.RunningTaskCount)
	e.Equal(int64(1), metrics.ScheduledTaskCount)
}

func (e *executionDAOTestSuite) TestRefreshStatus() {
	// contains tasks with status: success
	taskID01, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.SuccessStatus.String(),
		StatusCode:  job.SuccessStatus.Code(),
		ExtraAttrs:  "{}",
		EndTime:     time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID01)

	statusChanged, currentStatus, err := e.executionDAO.RefreshStatus(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.True(statusChanged)
	e.Equal(job.SuccessStatus.String(), currentStatus)
	execution, err := e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(job.SuccessStatus.String(), execution.Status)
	e.NotEmpty(execution.EndTime)

	// contains tasks with status: stopped
	taskID02, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.StoppedStatus.String(),
		StatusCode:  job.StoppedStatus.Code(),
		ExtraAttrs:  "{}",
		EndTime:     time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID02)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.True(statusChanged)
	e.Equal(job.StoppedStatus.String(), currentStatus)
	execution, err = e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(job.StoppedStatus.String(), execution.Status)
	e.NotEmpty(execution.EndTime)

	// contains tasks with status: error
	taskID03, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.ErrorStatus.String(),
		StatusCode:  job.ErrorStatus.Code(),
		ExtraAttrs:  "{}",
		EndTime:     time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID03)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.True(statusChanged)
	e.Equal(job.ErrorStatus.String(), currentStatus)
	execution, err = e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(job.ErrorStatus.String(), execution.Status)
	e.NotEmpty(execution.EndTime)

	// contains tasks with status: pending, running, scheduled
	taskID04, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.PendingStatus.String(),
		StatusCode:  job.PendingStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID04)

	taskID05, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.RunningStatus.String(),
		StatusCode:  job.RunningStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID05)

	taskID06, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.ScheduledStatus.String(),
		StatusCode:  job.ScheduledStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID06)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.True(statusChanged)
	e.Equal(job.RunningStatus.String(), currentStatus)
	execution, err = e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(job.RunningStatus.String(), execution.Status)
	e.Empty(execution.EndTime)

	// add another running task, the status shouldn't be changed
	taskID07, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.RunningStatus.String(),
		StatusCode:  job.RunningStatus.Code(),
		ExtraAttrs:  "{}",
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, taskID07)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.False(statusChanged)
	e.Equal(job.RunningStatus.String(), currentStatus)
	execution, err = e.executionDAO.Get(e.ctx, e.executionID)
	e.Require().Nil(err)
	e.Equal(job.RunningStatus.String(), execution.Status)
	e.Empty(execution.EndTime)
}

func (e *executionDAOTestSuite) TestRefreshStatus_PreserveExecutionError() {
	// Case 1: execution is Error with StatusMessage, tasks all succeed
	execID1, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType:    "test",
		Trigger:       "test",
		ExtraAttrs:    "{}",
		Status:        job.ErrorStatus.String(),
		StatusMessage: "orig error message",
	})
	e.Require().Nil(err)
	defer e.executionDAO.Delete(e.ctx, execID1)

	task1, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.SuccessStatus.String(),
		StatusCode:  job.SuccessStatus.Code(),
		ExtraAttrs:  "{}",
		EndTime:     time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, task1)

	// re-point task to execID1
	err = e.taskDao.Update(e.ctx, &Task{ID: task1, ExecutionID: execID1}, "ExecutionID")
	e.Require().Nil(err)

	statusChanged, currentStatus, err := e.executionDAO.RefreshStatus(e.ctx, execID1)
	e.Require().Nil(err)
	e.False(statusChanged)
	e.Equal(job.ErrorStatus.String(), currentStatus)
	exec, err := e.executionDAO.Get(e.ctx, execID1)
	e.Require().Nil(err)
	e.Equal(job.ErrorStatus.String(), exec.Status)
	e.Equal("orig error message", exec.StatusMessage)

	// Case 2: execution is Error with StatusMessage, tasks all stopped
	execID2, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType:    "test",
		Trigger:       "test",
		ExtraAttrs:    "{}",
		Status:        job.ErrorStatus.String(),
		StatusMessage: "orig error message",
	})
	e.Require().Nil(err)
	defer e.executionDAO.Delete(e.ctx, execID2)

	task2, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: e.executionID,
		Status:      job.StoppedStatus.String(),
		StatusCode:  job.StoppedStatus.Code(),
		ExtraAttrs:  "{}",
		EndTime:     time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, task2)

	err = e.taskDao.Update(e.ctx, &Task{ID: task2, ExecutionID: execID2}, "ExecutionID")
	e.Require().Nil(err)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, execID2)
	e.Require().Nil(err)
	e.False(statusChanged)
	e.Equal(job.ErrorStatus.String(), currentStatus)
	exec, err = e.executionDAO.Get(e.ctx, execID2)
	e.Require().Nil(err)
	e.Equal(job.ErrorStatus.String(), exec.Status)
	e.Equal("orig error message", exec.StatusMessage)

	// Case 3: execution is Error with StatusMessage, a task failed with its own message
	execID3, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType:    "test",
		Trigger:       "test",
		ExtraAttrs:    "{}",
		Status:        job.ErrorStatus.String(),
		StatusMessage: "orig error message",
	})
	e.Require().Nil(err)
	defer e.executionDAO.Delete(e.ctx, execID3)

	task3, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID:   e.executionID,
		Status:        job.ErrorStatus.String(),
		StatusCode:    job.ErrorStatus.Code(),
		StatusMessage: "task failed error",
		ExtraAttrs:    "{}",
		EndTime:       time.Now(),
	})
	e.Require().Nil(err)
	defer e.taskDao.Delete(e.ctx, task3)

	err = e.taskDao.Update(e.ctx, &Task{ID: task3, ExecutionID: execID3}, "ExecutionID")
	e.Require().Nil(err)

	statusChanged, currentStatus, err = e.executionDAO.RefreshStatus(e.ctx, execID3)
	e.Require().Nil(err)
	e.Equal(job.ErrorStatus.String(), currentStatus)
	exec, err = e.executionDAO.Get(e.ctx, execID3)
	e.Require().Nil(err)
	e.Equal(job.ErrorStatus.String(), exec.Status)
	e.Equal("orig error message\ntask failed error", exec.StatusMessage)
}

func (e *executionDAOTestSuite) TestAsyncRefreshStatus() {
	e.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	client, err := libredis.GetHarborClient()
	e.Require().NoError(err)

	member := buildExecStatusOutdateMember(e.executionID, "GC")
	defer client.SRem(e.ctx, execStatusOutdateSetKey, member)

	err = e.executionDAO.AsyncRefreshStatus(e.ctx, e.executionID, "GC")
	e.NoError(err)

	isMember, err := client.SIsMember(e.ctx, execStatusOutdateSetKey, member).Result()
	e.NoError(err)
	e.True(isMember)

	// AsyncRefreshStatus should be idempotent: calling it again does not fail
	// and does not introduce duplicates (SET semantic).
	err = e.executionDAO.AsyncRefreshStatus(e.ctx, e.executionID, "GC")
	e.NoError(err)
	count, err := client.SCard(e.ctx, execStatusOutdateSetKey).Result()
	e.NoError(err)
	e.GreaterOrEqual(count, int64(1))
}

func (e *executionDAOTestSuite) TestScanAndRefreshOutdateStatus() {
	e.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	// create execution1 with 1 running task
	id1, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType: "test1",
		Trigger:    "test",
		ExtraAttrs: `{"key":"value"}`,
	})
	e.NoError(err)
	defer e.executionDAO.Delete(e.ctx, id1)

	tid1, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: id1,
		Status:      job.RunningStatus.String(),
		StatusCode:  job.RunningStatus.Code(),
		ExtraAttrs:  `{}`,
	})
	e.NoError(err)
	defer e.taskDao.Delete(e.ctx, tid1)

	// create execution1 with 1 error task
	id2, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType: "test2",
		Trigger:    "test",
		ExtraAttrs: `{"key":"value"}`,
	})
	e.NoError(err)
	defer e.executionDAO.Delete(e.ctx, id2)

	tid2, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: id2,
		Status:      job.ErrorStatus.String(),
		StatusCode:  job.ErrorStatus.Code(),
		ExtraAttrs:  `{}`,
	})
	e.NoError(err)
	defer e.taskDao.Delete(e.ctx, tid2)

	// async refresh the status
	err = e.executionDAO.AsyncRefreshStatus(e.ctx, id1, "GC")
	e.NoError(err)
	err = e.executionDAO.AsyncRefreshStatus(e.ctx, id2, "GC")
	e.NoError(err)
	// test scan out and refresh
	scanAndRefreshOutdateStatus(e.ctx)
	exec1, err := e.executionDAO.Get(e.ctx, id1)
	e.NoError(err)
	e.Equal(job.RunningStatus.String(), exec1.Status)
	exec2, err := e.executionDAO.Get(e.ctx, id2)
	e.NoError(err)
	e.Equal(job.ErrorStatus.String(), exec2.Status)
}

func (e *executionDAOTestSuite) TestMigrateLegacyOutdateKeys() {
	e.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	client, err := libredis.GetHarborClient()
	e.Require().NoError(err)

	id, err := e.executionDAO.Create(e.ctx, &Execution{
		VendorType: "GC",
		Trigger:    "test",
		ExtraAttrs: `{"key":"value"}`,
	})
	e.Require().NoError(err)
	defer e.executionDAO.Delete(e.ctx, id)
	tid, err := e.taskDao.Create(e.ctx, &Task{
		ExecutionID: id,
		Status:      job.SuccessStatus.String(),
		StatusCode:  job.SuccessStatus.Code(),
		ExtraAttrs:  `{}`,
	})
	e.Require().NoError(err)
	defer e.taskDao.Delete(e.ctx, tid)

	// the keys written by the previous versions
	legacyKey := fmt.Sprintf("cache:execution:id:%d:vendor:GC:status_outdate", id)
	invalidKey := "cache:execution:id:invalid:vendor:GC:status_outdate"
	e.Require().NoError(client.Set(e.ctx, legacyKey, "", 0).Err())
	e.Require().NoError(client.Set(e.ctx, invalidKey, "", 0).Err())
	defer client.Del(e.ctx, legacyKey, invalidKey)

	defer func(at time.Time) { legacyExecStatusOutdateMigrateAt = at }(legacyExecStatusOutdateMigrateAt)
	legacyExecStatusOutdateMigrated.Store(false)

	// not migrated before the migration time
	legacyExecStatusOutdateMigrateAt = time.Now().Add(time.Hour)
	scanAndRefreshOutdateStatus(e.ctx)
	e.Equal(int64(1), client.Exists(e.ctx, legacyKey).Val())
	exec, err := e.executionDAO.Get(e.ctx, id)
	e.Require().NoError(err)
	e.Empty(exec.Status)

	// migrated and refreshed after the migration time
	legacyExecStatusOutdateMigrateAt = time.Now().Add(-time.Second)
	scanAndRefreshOutdateStatus(e.ctx)
	e.True(legacyExecStatusOutdateMigrated.Load())
	e.Zero(client.Exists(e.ctx, legacyKey).Val())
	e.Equal(int64(1), client.Exists(e.ctx, invalidKey).Val())
	exec, err = e.executionDAO.Get(e.ctx, id)
	e.Require().NoError(err)
	e.Equal(job.SuccessStatus.String(), exec.Status)
}

func (e *executionDAOTestSuite) TestMigrateLegacyOutdateKeysRetriedOnFailure() {
	e.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	client, err := libredis.GetHarborClient()
	e.Require().NoError(err)

	legacyKey := "cache:execution:id:999124:vendor:GC:status_outdate"
	e.Require().NoError(client.Set(e.ctx, legacyKey, "", 0).Err())
	defer client.Del(e.ctx, legacyKey)

	defer func(at time.Time) { legacyExecStatusOutdateMigrateAt = at }(legacyExecStatusOutdateMigrateAt)
	legacyExecStatusOutdateMigrateAt = time.Now().Add(-time.Second)
	legacyExecStatusOutdateMigrated.Store(false)

	ctx, cancel := context.WithCancel(e.ctx)
	cancel()
	scanAndRefreshOutdateStatus(ctx)
	e.False(legacyExecStatusOutdateMigrated.Load())
	e.Equal(int64(1), client.Exists(e.ctx, legacyKey).Val())

	scanAndRefreshOutdateStatus(e.ctx)
	e.True(legacyExecStatusOutdateMigrated.Load())
	e.Zero(client.Exists(e.ctx, legacyKey).Val())
}

type fakeRefreshExecDAO struct {
	ExecutionDAO
	current, refreshed string
}

func (f *fakeRefreshExecDAO) RefreshStatus(context.Context, int64) (bool, string, error) {
	f.refreshed = f.current
	return true, f.current, nil
}

// beforeCmdHook calls before ahead of each command other than SCAN
type beforeCmdHook struct {
	before func()
}

func (h *beforeCmdHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *beforeCmdHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *beforeCmdHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() != "scan" {
			h.before()
		}
		return next(ctx, cmd)
	}
}

func (e *executionDAOTestSuite) TestMigrateLegacyOutdateKeyIsAtomic() {
	e.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	client, err := libredis.GetHarborClient()
	e.Require().NoError(err)

	legacyKey := "cache:execution:id:999123:vendor:GC:status_outdate"
	e.Require().NoError(client.Del(e.ctx, execStatusOutdateSetKey).Err())
	e.Require().NoError(client.Set(e.ctx, legacyKey, "", 0).Err())
	defer client.Del(e.ctx, legacyKey, execStatusOutdateSetKey)

	defer func(d ExecutionDAO) { ExecDAO = d }(ExecDAO)
	fake := &fakeRefreshExecDAO{current: "v0"}
	ExecDAO = fake
	defer func(at time.Time) { legacyExecStatusOutdateMigrateAt = at }(legacyExecStatusOutdateMigrateAt)
	legacyExecStatusOutdateMigrateAt = time.Now().Add(time.Hour)

	migrationClient := redis.NewClient(&redis.Options{Addr: client.Options().Addr, DB: client.Options().DB})
	defer migrationClient.Close()
	n := 0
	migrationClient.AddHook(&beforeCmdHook{before: func() {
		// a new instance consumes the set, then an instance of the previous version
		// updates the status and writes the legacy key
		scanAndRefreshOutdateStatus(e.ctx)
		n++
		fake.current = fmt.Sprintf("v%d", n)
		e.Require().NoError(client.Set(e.ctx, legacyKey, "", 0).Err())
	}})
	e.Require().NoError(migrateLegacyOutdateKeys(e.ctx, migrationClient))

	scanAndRefreshOutdateStatus(e.ctx)
	e.Equal(fake.current, fake.refreshed, "the latest status should be refreshed")
}

func TestExecutionDAOSuite(t *testing.T) {
	suite.Run(t, &executionDAOTestSuite{})
}

func Test_buildInClauseSQLForExtraAttrs(t *testing.T) {
	tests := []struct {
		name string
		args []jsonbStru
		want string
	}{
		{"extra_attrs.", []jsonbStru{}, ""},
		{"extra_attrs.", []jsonbStru{{}}, ""},
		{"extra_attrs.id", []jsonbStru{{
			keyPrefix: "extra_attrs.",
			key:       "extra_attrs.id",
			value:     "1",
		}}, "select id from execution where extra_attrs->>?=?"},
		{"extra_attrs.artifact.digest", []jsonbStru{{
			keyPrefix: "extra_attrs.",
			key:       "extra_attrs.artifact.digest",
			value:     "sha256:1234",
		}}, "select id from execution where extra_attrs->?->>?=?"},
		{"extra_attrs.a.b.c", []jsonbStru{{
			keyPrefix: "extra_attrs.",
			key:       "extra_attrs.a.b.c",
			value:     "test_value_1",
		}, {
			keyPrefix: "extra_attrs.",
			key:       "extra_attrs.d.e",
			value:     "test_value_2",
		}}, "select id from execution where extra_attrs->?->?->>?=? and extra_attrs->?->>?=?"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := buildInClauseSQLForExtraAttrs(tt.args); got != tt.want {
				t.Errorf("buildInClauseSQLForExtraAttrs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseExecStatusOutdateMember(t *testing.T) {
	type args struct {
		member string
	}
	tests := []struct {
		name       string
		args       args
		wantID     int64
		wantVendor string
		wantErr    bool
	}{
		{"empty", args{""}, 0, "", true},
		{"missing vendor", args{"100"}, 0, "", true},
		{"empty vendor", args{"100:"}, 0, "", true},
		{"invalid execution id", args{"12abc:GC"}, 0, "", true},
		{"valid 1", args{"100:GARBAGE_COLLECTION"}, 100, "GARBAGE_COLLECTION", false},
		{"valid 2", args{"200:P2P_PREHEAT"}, 200, "P2P_PREHEAT", false},
		// vendor itself may not legally contain ':' but parser uses SplitN(2) so
		// extra colons would be kept inside vendor part; guard the common case.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseExecStatusOutdateMember(tt.args.member)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseExecStatusOutdateMember() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantID {
				t.Errorf("parseExecStatusOutdateMember() got = %v, want %v", got, tt.wantID)
			}
			if got1 != tt.wantVendor {
				t.Errorf("parseExecStatusOutdateMember() got1 = %v, want %v", got1, tt.wantVendor)
			}
		})
	}
}

func Test_buildExecStatusOutdateMember(t *testing.T) {
	got := buildExecStatusOutdateMember(123, "GARBAGE_COLLECTION")
	if got != "123:GARBAGE_COLLECTION" {
		t.Errorf("buildExecStatusOutdateMember() = %v, want %v", got, "123:GARBAGE_COLLECTION")
	}
}

func TestAggregateTaskStatusMessages(t *testing.T) {
	// empty tasks
	if got := aggregateTaskStatusMessages(nil); got != "" {
		t.Errorf("expected empty string, got %v", got)
	}
	if got := aggregateTaskStatusMessages([]*Task{{StatusMessage: ""}, {StatusMessage: "   "}}); got != "" {
		t.Errorf("expected empty string, got %v", got)
	}

	// 1 task
	t1 := []*Task{{StatusMessage: "413 Quota Exceeded"}}
	if got := aggregateTaskStatusMessages(t1); got != "413 Quota Exceeded" {
		t.Errorf("expected '413 Quota Exceeded', got %v", got)
	}

	// multiple tasks with same message
	tSame := []*Task{
		{StatusMessage: "413 Quota Exceeded"},
		{StatusMessage: "413 Quota Exceeded"},
	}
	expectedSame := "2 tasks failed with error: 413 Quota Exceeded"
	if got := aggregateTaskStatusMessages(tSame); got != expectedSame {
		t.Errorf("expected %q, got %q", expectedSame, got)
	}

	// multiple tasks with same multiline message
	tSameMulti := []*Task{
		{StatusMessage: "line 1\nline 2"},
		{StatusMessage: "line 1\nline 2"},
	}
	expectedSameMulti := "2 tasks failed with error:\nline 1\nline 2"
	if got := aggregateTaskStatusMessages(tSameMulti); got != expectedSameMulti {
		t.Errorf("expected %q, got %q", expectedSameMulti, got)
	}

	// multiple tasks with distinct single-line messages
	tDiffSingle := []*Task{
		{StatusMessage: "error A"},
		{StatusMessage: "error B"},
	}
	expectedDiffSingle := "2 tasks failed:\n- error A\n- error B"
	if got := aggregateTaskStatusMessages(tDiffSingle); got != expectedDiffSingle {
		t.Errorf("expected %q, got %q", expectedDiffSingle, got)
	}

	// multiple tasks with distinct multiline messages
	tDiffMulti := []*Task{
		{StatusMessage: "log A1\nlog A2"},
		{StatusMessage: "log B1\nlog B2"},
	}
	expectedDiffMulti := "2 tasks failed with 2 distinct errors:\nlog A1\nlog A2\n---\nlog B1\nlog B2"
	if got := aggregateTaskStatusMessages(tDiffMulti); got != expectedDiffMulti {
		t.Errorf("expected %q, got %q", expectedDiffMulti, got)
	}

	// length truncation
	longMsg := strings.Repeat("x", 5000)
	tLong := []*Task{{StatusMessage: longMsg}}
	gotLong := aggregateTaskStatusMessages(tLong)
	if len(gotLong) > 4096 || !strings.HasSuffix(gotLong, "...") {
		t.Errorf("expected truncated string <= 4096 ending with ..., got length %d", len(gotLong))
	}

	// UTF-8 length truncation
	longUTF8 := strings.Repeat("复制镜像失败：配额超限。\n", 200) // ~7800 bytes
	tLongUTF8 := []*Task{{StatusMessage: longUTF8}}
	gotLongUTF8 := aggregateTaskStatusMessages(tLongUTF8)
	if len(gotLongUTF8) > 4096 || !strings.HasSuffix(gotLongUTF8, "...") {
		t.Errorf("expected truncated string <= 4096 ending with ..., got length %d", len(gotLongUTF8))
	}
	if !utf8.ValidString(gotLongUTF8) {
		t.Errorf("expected valid UTF-8 string, but got invalid UTF-8")
	}

	// bounded query sampling with totalCount > len(tasks)
	expectedTotalSame := "100 tasks failed with error: 413 Quota Exceeded"
	if got := aggregateTaskStatusMessages(tSame, 100); got != expectedTotalSame {
		t.Errorf("expected %q, got %q", expectedTotalSame, got)
	}

	expectedTotalSample := "100 tasks failed (showing 2 error samples):\n- error A\n- error B"
	if got := aggregateTaskStatusMessages(tDiffSingle, 100); got != expectedTotalSample {
		t.Errorf("expected %q, got %q", expectedTotalSample, got)
	}

	expectedTotalMulti := "100 tasks failed with at least 2 distinct errors:\nlog A1\nlog A2\n---\nlog B1\nlog B2"
	if got := aggregateTaskStatusMessages(tDiffMulti, 100); got != expectedTotalMulti {
		t.Errorf("expected %q, got %q", expectedTotalMulti, got)
	}
}

func TestMergeStatusMessages(t *testing.T) {
	// both empty
	if got := mergeStatusMessages("", ""); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
	if got := mergeStatusMessages("   ", "   "); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}

	// one empty
	if got := mergeStatusMessages("exec error", ""); got != "exec error" {
		t.Errorf("expected 'exec error', got %q", got)
	}
	if got := mergeStatusMessages("", "task error"); got != "task error" {
		t.Errorf("expected 'task error', got %q", got)
	}

	// identical
	if got := mergeStatusMessages("same error", "same error"); got != "same error" {
		t.Errorf("expected 'same error', got %q", got)
	}

	// execMsg contains taskMsg
	merged := "execution failed: timeout\n1 tasks failed with error: 413 Quota Exceeded"
	taskMsg := "1 tasks failed with error: 413 Quota Exceeded"
	if got := mergeStatusMessages(merged, taskMsg); got != merged {
		t.Errorf("expected %q, got %q", merged, got)
	}

	// taskMsg contains execMsg
	if got := mergeStatusMessages("413 Quota Exceeded", "2 tasks failed:\n- 413 Quota Exceeded\n- unknown"); got != "2 tasks failed:\n- 413 Quota Exceeded\n- unknown" {
		t.Errorf("expected full taskMsg, got %q", got)
	}

	// distinct messages merged
	execMsg := "failed to replicate: connection refused"
	expectedMerged := "failed to replicate: connection refused\n1 tasks failed with error: 413 Quota Exceeded"
	if got := mergeStatusMessages(execMsg, taskMsg); got != expectedMerged {
		t.Errorf("expected %q, got %q", expectedMerged, got)
	}

	// repeated merge is idempotent
	if got := mergeStatusMessages(expectedMerged, taskMsg); got != expectedMerged {
		t.Errorf("expected idempotent merge to return %q, got %q", expectedMerged, got)
	}

	// truncation test
	longExec := strings.Repeat("e", 3000)
	longTask := strings.Repeat("t", 3000)
	gotLong := mergeStatusMessages(longExec, longTask)
	if len(gotLong) > 4096 || !strings.HasSuffix(gotLong, "...") {
		t.Errorf("expected truncated string <= 4096 ending with ..., got length %d", len(gotLong))
	}
}
