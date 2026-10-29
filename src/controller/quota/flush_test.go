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

package quota

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/common"
	"github.com/goharbor/harbor/src/lib/cache"
	"github.com/goharbor/harbor/src/lib/config"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	libredis "github.com/goharbor/harbor/src/lib/redis"
	"github.com/goharbor/harbor/src/pkg/quota"
	"github.com/goharbor/harbor/src/pkg/quota/driver"
	"github.com/goharbor/harbor/src/pkg/quota/types"
	ormtesting "github.com/goharbor/harbor/src/testing/lib/orm"
	"github.com/goharbor/harbor/src/testing/mock"
	quotatesting "github.com/goharbor/harbor/src/testing/pkg/quota"
	drivertesting "github.com/goharbor/harbor/src/testing/pkg/quota/driver"
)

type FlushTestSuite struct {
	suite.Suite

	ctx    context.Context
	client *redis.Client

	mu      sync.Mutex
	updated map[string]int64
}

func (s *FlushTestSuite) SetupSuite() {
	s.T().Setenv("_REDIS_URL_HARBOR", "redis://localhost:6379/0")
	client, err := libredis.GetHarborClient()
	s.Require().NoError(err)
	s.client = client
	s.ctx = context.Background()
}

func (s *FlushTestSuite) SetupTest() {
	s.Require().NoError(s.client.Del(s.ctx, quotaDirtySetKey, quotaFlushingSetKey, quotaFlushLockKey).Err())
	s.updated = map[string]int64{}
}

func (s *FlushTestSuite) TearDownTest() {
	s.Require().NoError(s.client.Del(s.ctx, quotaDirtySetKey, quotaFlushingSetKey, quotaFlushLockKey).Err())
}

func (s *FlushTestSuite) newFlusher(update func(ctx context.Context, q *quota.Quota) error) *quotaFlusher {
	if update == nil {
		update = s.record
	}
	return &quotaFlusher{
		getClient:       libredis.GetHarborClient,
		update:          update,
		lockTTL:         quotaFlushLockTTL,
		batchSize:       quotaFlushBatchSize,
		keyTimeout:      quotaFlushKeyTimeout,
		nextLegacySweep: time.Now().Add(time.Hour),
		legacySweepEnd:  time.Now().Add(2 * time.Hour),
	}
}

func (s *FlushTestSuite) record(_ context.Context, q *quota.Quota) error {
	used, err := q.GetUsed()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updated[q.ReferenceID] = used[types.ResourceStorage]
	return nil
}

func (s *FlushTestSuite) flushed(referenceID string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.updated[referenceID]
	return v, ok
}

func (s *FlushTestSuite) setQuota(referenceID string, used int64, dirty bool) string {
	key := fmt.Sprintf("%smock:%s", quotaKeyPrefix, referenceID)
	q := &quota.Quota{Reference: "mock", ReferenceID: referenceID}
	q.SetHard(types.ResourceList{types.ResourceStorage: 1000})
	q.SetUsed(types.ResourceList{types.ResourceStorage: used})
	val, err := cache.DefaultCodec().Encode(q)
	s.Require().NoError(err)
	_, err = s.client.TxPipelined(s.ctx, func(p redis.Pipeliner) error {
		p.Set(s.ctx, key, val, time.Minute)
		if dirty {
			p.SAdd(s.ctx, quotaDirtySetKey, key)
		}
		return nil
	})
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.client.Del(s.ctx, key) })
	return key
}

func (s *FlushTestSuite) dirtyKeys() []string {
	keys, err := s.client.SMembers(s.ctx, quotaDirtySetKey).Result()
	s.Require().NoError(err)
	return keys
}

func (s *FlushTestSuite) flushingKeys() []string {
	keys, err := s.client.SMembers(s.ctx, quotaFlushingSetKey).Result()
	s.Require().NoError(err)
	return keys
}

func (s *FlushTestSuite) TestUpdateUsageByRedisMarksDirty() {
	d := &drivertesting.Driver{}
	driver.Register("mock", d)
	mgr := &quotatesting.Manager{}
	ctl := &controller{quotaMgr: mgr}

	referenceID := uuid.NewString()
	hard := types.ResourceList{types.ResourceStorage: 100}
	q := &quota.Quota{Reference: "mock", ReferenceID: referenceID, Hard: hard.String(), Used: types.Zero(hard).String()}
	mock.OnAnything(mgr, "GetByRef").Return(q, nil)
	mock.OnAnything(d, "CalculateUsage").Return(types.ResourceList{types.ResourceStorage: 10}, nil)

	ctx := orm.NewContext(context.TODO(), &ormtesting.FakeOrmer{})
	s.Require().NoError(ctl.updateUsageByRedis(ctx, "mock", referenceID, reserveResources(types.ResourceList{types.ResourceStorage: 5})))

	key := fmt.Sprintf("%smock:%s", quotaKeyPrefix, referenceID)
	defer s.client.Del(s.ctx, key)
	s.Contains(s.dirtyKeys(), key)

	s.newFlusher(nil).flush(s.ctx)
	used, ok := s.flushed(referenceID)
	s.True(ok)
	s.Equal(int64(15), used)
	s.Empty(s.dirtyKeys())
}

func (s *FlushTestSuite) TestFlushDrainsDirtyKeys() {
	ids := make([]string, 0, 250)
	for i := range 250 {
		id := uuid.NewString()
		ids = append(ids, id)
		s.setQuota(id, int64(i), true)
	}
	// not dirty, must not be flushed
	clean := uuid.NewString()
	s.setQuota(clean, 1, false)
	// dirty but expired, must be dropped
	s.Require().NoError(s.client.SAdd(s.ctx, quotaDirtySetKey, quotaKeyPrefix+"mock:expired").Err())

	s.newFlusher(nil).flush(s.ctx)

	for i, id := range ids {
		used, ok := s.flushed(id)
		s.True(ok)
		s.Equal(int64(i), used)
	}
	_, ok := s.flushed(clean)
	s.False(ok)
	s.Empty(s.dirtyKeys())
	s.Zero(s.client.Exists(s.ctx, quotaFlushLockKey).Val())
}

func (s *FlushTestSuite) TestUpdateDuringFlushIsNotLost() {
	id := uuid.NewString()
	s.setQuota(id, 1, true)

	var once sync.Once
	f := s.newFlusher(func(ctx context.Context, q *quota.Quota) error {
		// the quota is updated in redis after its value has been read by the flusher
		once.Do(func() { s.setQuota(id, 2, true) })
		return s.record(ctx, q)
	})

	f.flush(s.ctx)
	used, _ := s.flushed(id)
	s.Equal(int64(1), used)
	// the key is still dirty and the latest value is flushed in the next round
	s.Len(s.dirtyKeys(), 1)

	f.flush(s.ctx)
	used, _ = s.flushed(id)
	s.Equal(int64(2), used)
	s.Empty(s.dirtyKeys())
}

func (s *FlushTestSuite) TestFailedKeysAreRetried() {
	failedID, deletedID := uuid.NewString(), uuid.NewString()
	failedKey := s.setQuota(failedID, 1, true)
	s.setQuota(deletedID, 1, true)

	f := s.newFlusher(func(_ context.Context, q *quota.Quota) error {
		if q.ReferenceID == deletedID {
			return errors.NotFoundError(nil)
		}
		return errors.New("db error")
	})
	f.flush(s.ctx)
	// the failed key is put back, the deleted one is dropped
	s.Equal([]string{failedKey}, s.dirtyKeys())

	f.update = s.record
	f.flush(s.ctx)
	_, ok := s.flushed(failedID)
	s.True(ok)
	s.Empty(s.dirtyKeys())
}

func (s *FlushTestSuite) TestSkipWhenLockHeldByOthers() {
	id := uuid.NewString()
	s.setQuota(id, 1, true)
	s.Require().NoError(s.client.Set(s.ctx, quotaFlushLockKey, "other", time.Minute).Err())

	s.newFlusher(nil).flush(s.ctx)

	_, ok := s.flushed(id)
	s.False(ok)
	s.Len(s.dirtyKeys(), 1)
	// the lock of others is not released
	s.Equal("other", s.client.Get(s.ctx, quotaFlushLockKey).Val())
}

func (s *FlushTestSuite) TestConcurrentFlushers() {
	for range 25 {
		s.setQuota(uuid.NewString(), 1, true)
	}

	var running, maxRunning, calls atomic.Int32
	var blocked atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	update := func(_ context.Context, _ *quota.Quota) error {
		n := running.Add(1)
		defer running.Add(-1)
		for {
			m := maxRunning.Load()
			if n <= m || maxRunning.CompareAndSwap(m, n) {
				break
			}
		}
		calls.Add(1)
		// block the first flusher in the middle of its first batch
		if blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	}
	newFlusher := func() *quotaFlusher {
		f := s.newFlusher(update)
		f.batchSize = 10
		return f
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		newFlusher().flush(s.ctx)
	}()
	<-entered

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			newFlusher().flush(s.ctx)
		}()
	}
	wg.Wait()
	s.Equal(int32(1), calls.Load(), "no other flusher should run while the lock is held")

	close(release)
	<-done
	s.Equal(int32(1), maxRunning.Load(), "only one flusher should run at a time")
	s.Equal(int32(25), calls.Load(), "each dirty key should be flushed exactly once")
	s.Empty(s.dirtyKeys())
	s.Empty(s.flushingKeys())
}

func (s *FlushTestSuite) TestStaleWriteAfterLockLostIsRewritten() {
	id := uuid.NewString()
	key := s.setQuota(id, 1, true)

	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	a := s.newFlusher(func(ctx context.Context, q *quota.Quota) error {
		// a has read the value 1 and is blocked before writing it
		once.Do(func() {
			close(entered)
			<-resume
		})
		return s.record(ctx, q)
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.flush(s.ctx)
	}()
	<-entered

	// the lock of a expires, b takes over and writes the newer value
	s.Require().NoError(s.client.Del(s.ctx, quotaFlushLockKey).Err())
	s.setQuota(id, 2, true)
	b := s.newFlusher(nil)
	b.flush(s.ctx)
	used, _ := s.flushed(id)
	s.Require().Equal(int64(2), used)

	// a writes the stale value after losing the lock
	close(resume)
	<-done
	used, _ = s.flushed(id)
	s.Require().Equal(int64(1), used)
	s.Contains(s.dirtyKeys(), key, "the key should be marked dirty for the latest value to be written again")

	b.flush(s.ctx)
	used, _ = s.flushed(id)
	s.Equal(int64(2), used)
	s.Empty(s.dirtyKeys())
	s.Empty(s.flushingKeys())
}

func (s *FlushTestSuite) TestCrashedFlushIsRecovered() {
	ids := make([]string, 0, 10)
	for range 10 {
		id := uuid.NewString()
		ids = append(ids, id)
		s.setQuota(id, 7, true)
	}

	// the flusher dies after claiming the keys and before flushing any of them
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.newFlusher(func(context.Context, *quota.Quota) error {
			runtime.Goexit()
			return nil
		}).flush(s.ctx)
	}()
	<-done
	s.Empty(s.dirtyKeys())
	s.Len(s.flushingKeys(), 10)

	s.newFlusher(nil).flush(s.ctx)
	for _, id := range ids {
		used, ok := s.flushed(id)
		s.True(ok)
		s.Equal(int64(7), used)
	}
	s.Empty(s.dirtyKeys())
	s.Empty(s.flushingKeys())
}

// failAfterHook runs the matching commands but reports an error to the client
type failAfterHook struct {
	match func(cmd redis.Cmder) bool
}

func (h *failAfterHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *failAfterHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *failAfterHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := next(ctx, cmd); err != nil || !h.match(cmd) {
			return err
		}
		return errors.New("connection reset")
	}
}

func (s *FlushTestSuite) TestAmbiguousClaimIsRecovered() {
	id := uuid.NewString()
	s.setQuota(id, 7, true)

	s.Require().NoError(claimScript.Load(s.ctx, s.client).Err())
	client := redis.NewClient(&redis.Options{Addr: s.client.Options().Addr, DB: s.client.Options().DB})
	defer client.Close()
	client.AddHook(&failAfterHook{match: func(cmd redis.Cmder) bool {
		return cmd.Name() == "evalsha" && cmd.Args()[1] == claimScript.Hash()
	}})

	// the keys are claimed but the client sees an error
	f := s.newFlusher(nil)
	f.getClient = func() (*redis.Client, error) { return client, nil }
	f.flush(s.ctx)
	_, ok := s.flushed(id)
	s.False(ok)
	s.Empty(s.dirtyKeys())

	s.newFlusher(nil).flush(s.ctx)
	used, ok := s.flushed(id)
	s.True(ok)
	s.Equal(int64(7), used)
	s.Empty(s.flushingKeys())
}

func (s *FlushTestSuite) TestSlowQuotaDoesNotExpireOthers() {
	for i := range 2 {
		key := s.setQuota(uuid.NewString(), int64(i), true)
		s.Require().NoError(s.client.PExpire(s.ctx, key, 500*time.Millisecond).Err())
	}

	var slowKey string
	var once sync.Once
	f := s.newFlusher(func(ctx context.Context, q *quota.Quota) error {
		slow := false
		once.Do(func() {
			slow = true
			slowKey = quotaKeyPrefix + "mock:" + q.ReferenceID
		})
		if slow {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}
		return s.record(ctx, q)
	})
	f.keyTimeout = 100 * time.Millisecond
	f.flush(s.ctx)

	s.Len(s.updated, 1, "the other quota should be flushed before it expires")
	s.Equal([]string{slowKey}, s.dirtyKeys(), "the slow quota should be retried")
}

func (s *FlushTestSuite) TestUpdateStopsRetryingWhenContextDone() {
	mgr := &quotatesting.Manager{}
	ctl := &controller{quotaMgr: mgr}
	q := &quota.Quota{Reference: "mock", ReferenceID: uuid.NewString()}
	q.SetHard(types.ResourceList{types.ResourceStorage: 100})
	q.SetUsed(types.ResourceList{types.ResourceStorage: 1})
	mock.OnAnything(mgr, "GetByRef").Return(q, nil)
	mock.OnAnything(mgr, "Update").Return(orm.ErrOptimisticLock)

	ctx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- ctl.Update(ctx, q) }()
	select {
	case err := <-errCh:
		s.ErrorIs(err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		s.Fail("Update should stop retrying once the context is done")
	}
}

func (s *FlushTestSuite) TestStopWhenLockLost() {
	for range 10 {
		s.setQuota(uuid.NewString(), 1, true)
	}

	var calls atomic.Int32
	f := s.newFlusher(func(_ context.Context, _ *quota.Quota) error {
		if calls.Add(1) == 1 {
			// another instance takes over the lock, e.g. after the lock expired
			s.client.Set(s.ctx, quotaFlushLockKey, "other", time.Minute)
			time.Sleep(200 * time.Millisecond)
		}
		return nil
	})
	f.lockTTL = 150 * time.Millisecond
	f.flush(s.ctx)

	s.Equal(int32(1), calls.Load())
	// the keys not flushed are put back, and the flushed one is marked dirty again
	// as it may have overwritten the value written by the new holder
	s.Len(s.dirtyKeys(), 10)
	// the lock of others is not released
	s.Equal("other", s.client.Get(s.ctx, quotaFlushLockKey).Val())
}

func (s *FlushTestSuite) TestLockIsExtended() {
	s.setQuota(uuid.NewString(), 1, true)

	var held bool
	f := s.newFlusher(func(_ context.Context, _ *quota.Quota) error {
		// longer than the lock ttl
		time.Sleep(400 * time.Millisecond)
		held = s.client.Exists(s.ctx, quotaFlushLockKey).Val() == 1
		return nil
	})
	f.lockTTL = 150 * time.Millisecond
	f.flush(s.ctx)
	s.True(held, "the lock should be extended while flushing")
	s.Empty(s.dirtyKeys())
}

func (s *FlushTestSuite) TestSweepLegacyKeys() {
	id := uuid.NewString()
	s.setQuota(id, 7, false)

	f := s.newFlusher(nil)
	f.flush(s.ctx)
	_, ok := s.flushed(id)
	s.False(ok, "legacy keys are not swept before the sweep time")

	f.nextLegacySweep = time.Now().Add(-time.Second)
	f.flush(s.ctx)
	used, ok := s.flushed(id)
	s.True(ok)
	s.Equal(int64(7), used)
	s.True(f.nextLegacySweep.After(time.Now()))

	// written by a previous version after the sweep
	s.setQuota(id, 8, false)
	f.flush(s.ctx)
	used, _ = s.flushed(id)
	s.Equal(int64(7), used, "legacy keys are not swept before the next sweep time")

	f.nextLegacySweep = time.Now().Add(-time.Second)
	f.flush(s.ctx)
	used, _ = s.flushed(id)
	s.Equal(int64(8), used)

	// no sweep after the end
	s.setQuota(id, 9, false)
	f.nextLegacySweep = time.Now().Add(-time.Second)
	f.legacySweepEnd = time.Now().Add(-2 * time.Second)
	f.flush(s.ctx)
	used, _ = s.flushed(id)
	s.Equal(int64(8), used)
}

func (s *FlushTestSuite) TestFailedLegacySweepIsRetried() {
	id := uuid.NewString()
	s.setQuota(id, 7, false)

	f := s.newFlusher(nil)
	f.nextLegacySweep = time.Now().Add(-time.Second)
	ctx, cancel := context.WithCancel(s.ctx)
	cancel()
	f.sweepLegacyKeys(ctx, s.client)
	s.Empty(s.dirtyKeys())

	f.flush(s.ctx)
	used, ok := s.flushed(id)
	s.True(ok, "the failed sweep should be retried")
	s.Equal(int64(7), used)
}

func (s *FlushTestSuite) TestQuotaFlushInterval() {
	mgr := config.DefaultMgr()
	defer mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, 30)

	s.Equal(defaultQuotaFlushInterval, quotaFlushInterval())

	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, "60")
	s.Equal(60*time.Second, quotaFlushInterval())

	// capped so that the quotas are flushed before they expire in redis
	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, "600")
	s.Equal(maxQuotaFlushInterval, quotaFlushInterval())

	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, 0)
	s.Equal(defaultQuotaFlushInterval, quotaFlushInterval())

	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, -1)
	s.Equal(defaultQuotaFlushInterval, quotaFlushInterval())

	// overflow time.Duration if converted before capping
	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, "9223372037")
	s.Equal(maxQuotaFlushInterval, quotaFlushInterval())
	mgr.Set(s.ctx, common.QuotaFlushIntervalSeconds, "36028797018963968")
	s.Equal(maxQuotaFlushInterval, quotaFlushInterval())
}

func TestFlushTestSuite(t *testing.T) {
	suite.Run(t, &FlushTestSuite{})
}
