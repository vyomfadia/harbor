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
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/goharbor/harbor/src/lib/cache"
	"github.com/goharbor/harbor/src/lib/config"
	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/metric"
	libredis "github.com/goharbor/harbor/src/lib/redis"
	"github.com/goharbor/harbor/src/pkg/quota"
)

const (
	// quotaKeyPrefix matches the prefix of the keys saved by cache.Default()
	quotaKeyPrefix = "cache:quota:"
	// quotaDirtySetKey holds the keys of the quotas updated in redis but not flushed to db yet.
	// It must not match the "cache:*quota:*" pattern scanned by the previous versions.
	quotaDirtySetKey = "quota:dirty"
	// quotaFlushingSetKey holds the keys claimed by the flush lock holder but not flushed yet,
	// the next holder moves them back to the dirty set in case the previous one died.
	quotaFlushingSetKey = "quota:flushing"
	// quotaFlushLockKey is the lock ensuring only one instance flushes at a time
	quotaFlushLockKey = "quota:flush_lock"
	// quotaFlushLockTTL is extended periodically while flushing
	quotaFlushLockTTL         = time.Minute
	quotaFlushBatchSize       = 100
	quotaFlushWorkers         = 8
	defaultQuotaFlushInterval = 30 * time.Second
	// maxQuotaFlushInterval is half of quotaExpireTimeout, quotas must be flushed before they expire
	maxQuotaFlushInterval = 150 * time.Second
	// quotaFlushKeyTimeout bounds flushing a single quota, so a slow one cannot hold up
	// the others until they expire
	quotaFlushKeyTimeout = 5 * time.Second

	// The previous versions do not mark quotas dirty, so the quota keys are swept repeatedly
	// to cover a rolling upgrade. Sweeps must be less than quotaExpireTimeout - maxQuotaFlushInterval
	// apart to catch every key written by the previous versions before it expires.
	legacySweepInterval = 2 * time.Minute
	legacySweepDuration = 30 * time.Minute
	// quotaLegacySweepWindowKey exists while any instance is within its legacySweepDuration, so the
	// lock holder sweeps even if its own window has ended. It outlives the flush interval of the instances.
	quotaLegacySweepWindowKey = "quota:legacy_sweep_window"
	legacySweepWindowTTL      = 2 * maxQuotaFlushInterval
	// quotaLegacySweptKey exists for legacySweepInterval after a successful sweep
	quotaLegacySweptKey = "quota:legacy_swept"
)

var (
	// releaseLockScript deletes the lock only if it is still held by the token
	releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)
	// extendLockScript extends the lock only if it is still held by the token
	extendLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)
	// recoverScript moves the keys left in the flushing set back to the dirty set
	recoverScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return redis.error_reply("quota flush lock not held")
end
local keys = redis.call("SMEMBERS", KEYS[3])
for i = 1, #keys, 1000 do
	redis.call("SADD", KEYS[2], unpack(keys, i, math.min(i + 999, #keys)))
end
redis.call("DEL", KEYS[3])
return #keys`)
	// claimScript moves at most ARGV[2] keys from the dirty set to the flushing set
	claimScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
	return redis.error_reply("quota flush lock not held")
end
local keys = redis.call("SPOP", KEYS[2], ARGV[2])
if #keys > 0 then
	redis.call("SADD", KEYS[3], unpack(keys))
end
return keys`)
	// ackScript removes a flushed key from the flushing set. If the lock has been lost, a newer
	// value written by the new holder may have been overwritten, so the key is marked dirty
	// again for the latest value to be written.
	ackScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("SREM", KEYS[3], ARGV[2])
end
redis.call("SADD", KEYS[2], ARGV[2])
return -1`)
)

// quotaFlushInterval returns the configured flush interval, bounded by maxQuotaFlushInterval.
func quotaFlushInterval() time.Duration {
	seconds := config.GetQuotaFlushIntervalSeconds()
	if seconds <= 0 {
		log.Warningf("invalid quota flush interval %d seconds, use the default value %s", seconds, defaultQuotaFlushInterval)
		return defaultQuotaFlushInterval
	}

	// compare before converting, large values overflow time.Duration
	if seconds > int64(maxQuotaFlushInterval/time.Second) {
		log.Warningf("quota flush interval %d seconds exceeds the max value, use the max value %s", seconds, maxQuotaFlushInterval)
		return maxQuotaFlushInterval
	}

	return time.Duration(seconds) * time.Second
}

// quotaFlusher flushes the quotas updated in redis to db
type quotaFlusher struct {
	getClient  func() (*redis.Client, error)
	update     func(ctx context.Context, q *quota.Quota) error
	lockTTL    time.Duration
	batchSize  int64
	workers    int
	keyTimeout time.Duration
	// legacySweepEnd is the end of the legacy sweep window of this instance
	legacySweepEnd time.Time
}

func newQuotaFlusher() *quotaFlusher {
	return &quotaFlusher{
		getClient: libredis.GetHarborClient,
		update: func(ctx context.Context, q *quota.Quota) error {
			return Ctl.Update(ctx, q)
		},
		lockTTL:        quotaFlushLockTTL,
		batchSize:      quotaFlushBatchSize,
		workers:        quotaFlushWorkers,
		keyTimeout:     quotaFlushKeyTimeout,
		legacySweepEnd: time.Now().Add(legacySweepDuration),
	}
}

// flush flushes the quotas in the dirty set to db, only the instance holding the flush lock does the work
func (f *quotaFlusher) flush(ctx context.Context) {
	client, err := f.getClient()
	if err != nil {
		log.Errorf("failed to get the redis client for flushing quota, error: %v", err)
		return
	}

	if time.Now().Before(f.legacySweepEnd) {
		if err := client.Set(ctx, quotaLegacySweepWindowKey, "", legacySweepWindowTTL).Err(); err != nil {
			log.Errorf("failed to set the legacy quota sweep window, error: %v", err)
		}
	}

	token := uuid.NewString()
	acquired, err := client.SetNX(ctx, quotaFlushLockKey, token, f.lockTTL).Result()
	if err != nil {
		log.Errorf("failed to acquire the quota flush lock, error: %v", err)
		return
	}
	if !acquired {
		log.Debug("the quota flush lock is held by another instance, skip flushing")
		return
	}

	keepCtx, stopKeep := context.WithCancel(ctx)
	lost := f.keepLock(keepCtx, client, token)
	defer func() {
		stopKeep()
		if err := releaseLockScript.Run(context.WithoutCancel(ctx), client, []string{quotaFlushLockKey}, token).Err(); err != nil {
			log.Errorf("failed to release the quota flush lock, error: %v", err)
		}
	}()

	start := time.Now()
	f.sweepLegacyKeys(ctx, client)
	flushed, failed := f.drain(ctx, client, token, lost)
	metric.QuotaFlushDurSummary.Observe(time.Since(start).Seconds())
	metric.QuotaFlushKeysTotal.WithLabelValues("flushed").Add(float64(flushed))
	metric.QuotaFlushKeysTotal.WithLabelValues("failed").Add(float64(failed))
	if flushed+failed > 0 {
		log.Debugf("flushed quotas from redis to db, %d flushed, %d failed, took %s", flushed, failed, time.Since(start))
	}
}

// drain claims the keys from the dirty set and flushes them, failed keys are put back for the next round.
//
// A key is removed from the dirty set before its value is read, so an update during
// the flush marks it dirty again and no update is lost. A claimed key stays in the
// flushing set until it is flushed, so it is not lost if this instance dies.
func (f *quotaFlusher) drain(ctx context.Context, client *redis.Client, token string, lost *atomic.Bool) (flushed, failed int) {
	scriptKeys := []string{quotaFlushLockKey, quotaDirtySetKey, quotaFlushingSetKey}
	n, err := recoverScript.Run(ctx, client, scriptKeys, token).Int()
	if err != nil {
		log.Errorf("failed to recover the quota keys left by the previous flush, error: %v", err)
		return
	}
	if n > 0 {
		log.Infof("recovered %d quota keys left by the previous flush", n)
	}

	// keys marked dirty again during this round are left for the next round
	pending, err := client.SCard(ctx, quotaDirtySetKey).Result()
	if err != nil {
		log.Errorf("failed to get the size of the quota dirty set, error: %v", err)
		return
	}

	var (
		mu      sync.Mutex
		retries []string
	)
	// put back at the end of the round, so that failed keys are not claimed again in place of the others
	defer func() {
		if len(retries) == 0 {
			return
		}
		if err := requeue(context.WithoutCancel(ctx), client, retries); err != nil {
			log.Errorf("failed to put %d keys back to the quota dirty set, error: %v", len(retries), err)
		}
	}()

	for pending > 0 && !lost.Load() {
		keys, err := claimScript.Run(ctx, client, scriptKeys, token, min(pending, f.batchSize)).StringSlice()
		if err != nil {
			// the keys claimed despite the error are recovered in the next round
			log.Errorf("failed to claim keys from the quota dirty set, error: %v", err)
			return
		}
		if len(keys) == 0 {
			return
		}
		pending -= int64(len(keys))

		// concurrently, so that the timeouts of slow quotas do not add up until the others expire
		var wg sync.WaitGroup
		queue := make(chan string)
		for range min(f.workers, len(keys)) {
			wg.Go(func() {
				for key := range queue {
					if lost.Load() {
						// the lock is lost, give the key back for the next holder
						mu.Lock()
						retries = append(retries, key)
						mu.Unlock()
						continue
					}
					if err := f.flushKey(ctx, client, key); err != nil {
						log.Errorf("failed to flush quota %s, error: %v", key, err)
						mu.Lock()
						retries = append(retries, key)
						failed++
						mu.Unlock()
						continue
					}
					mu.Lock()
					flushed++
					mu.Unlock()
					// a failed ack leaves the key in the flushing set, it is flushed again in the next round
					res, err := ackScript.Run(context.WithoutCancel(ctx), client, scriptKeys, token, key).Int()
					if err != nil {
						log.Errorf("failed to remove quota %s from the flushing set, error: %v", key, err)
					} else if res < 0 {
						log.Warningf("lost the quota flush lock while flushing quota %s, stop flushing", key)
						lost.Store(true)
					}
				}
			})
		}
		for _, key := range keys {
			queue <- key
		}
		close(queue)
		wg.Wait()
	}

	return
}

// requeue moves the keys from the flushing set back to the dirty set
func requeue(ctx context.Context, client *redis.Client, keys []string) error {
	members := make([]any, 0, len(keys))
	for _, k := range keys {
		members = append(members, k)
	}
	_, err := client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SAdd(ctx, quotaDirtySetKey, members...)
		p.SRem(ctx, quotaFlushingSetKey, members...)
		return nil
	})
	return err
}

func (f *quotaFlusher) flushKey(ctx context.Context, client *redis.Client, key string) error {
	ctx, cancel := context.WithTimeout(ctx, f.keyTimeout)
	defer cancel()

	data, err := client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			// the key expired, nothing to flush, the usage will be recalculated on the next update
			log.Debugf("quota %s not found in redis, skip flushing", key)
			return nil
		}
		return err
	}

	q := &quota.Quota{}
	if err := cache.DefaultCodec().Decode(data, q); err != nil {
		// a broken value never succeeds, do not retry it
		log.Errorf("failed to decode quota %s, skip flushing, error: %v", key, err)
		return nil
	}

	if err := f.update(ctx, q); err != nil {
		if errors.IsNotFoundErr(err) {
			// the quota has been deleted
			log.Debugf("quota %s not found in db, skip flushing", key)
			return nil
		}
		return err
	}

	log.Debugf("successfully flushed quota: %s", key)
	return nil
}

// keepLock extends the flush lock until ctx is done, the returned flag is set once the lock is lost
func (f *quotaFlusher) keepLock(ctx context.Context, client *redis.Client, token string) *atomic.Bool {
	lost := &atomic.Bool{}
	go func() {
		ticker := time.NewTicker(f.lockTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				res, err := extendLockScript.Run(ctx, client, []string{quotaFlushLockKey}, token, f.lockTTL.Milliseconds()).Int()
				if ctx.Err() != nil {
					return
				}
				if err != nil || res == 0 {
					log.Warningf("lost the quota flush lock, stop flushing, error: %v", err)
					lost.Store(true)
					return
				}
			}
		}
	}()

	return lost
}

// sweepLegacyKeys marks all the quota keys in redis as dirty, at most once every legacySweepInterval
// across the instances, while any instance is within its sweep window. The previous versions did not
// track dirty keys, so the quotas they updated during a rolling upgrade would otherwise never be flushed.
// It is called by the lock holder only, a failed sweep is retried in the next round.
func (f *quotaFlusher) sweepLegacyKeys(ctx context.Context, client *redis.Client) {
	var inWindow, swept *redis.IntCmd
	_, err := client.Pipelined(ctx, func(p redis.Pipeliner) error {
		inWindow = p.Exists(ctx, quotaLegacySweepWindowKey)
		swept = p.Exists(ctx, quotaLegacySweptKey)
		return nil
	})
	if err != nil {
		log.Errorf("failed to check the legacy quota sweep, error: %v", err)
		return
	}
	if inWindow.Val() == 0 || swept.Val() == 1 {
		return
	}

	total := 0
	keys := make([]any, 0, 1000)
	addKeys := func() error {
		if len(keys) == 0 {
			return nil
		}
		if err := client.SAdd(ctx, quotaDirtySetKey, keys...).Err(); err != nil {
			return err
		}
		total += len(keys)
		keys = keys[:0]
		return nil
	}

	iter := client.Scan(ctx, 0, quotaKeyPrefix+"*", 1000).Iterator()
	for err == nil && iter.Next(ctx) {
		if keys = append(keys, iter.Val()); len(keys) >= 1000 {
			err = addKeys()
		}
	}
	if err == nil {
		err = iter.Err()
	}
	if err == nil {
		err = addKeys()
	}
	if err != nil {
		log.Errorf("failed to mark the legacy quota keys as dirty, will retry, error: %v", err)
		return
	}

	if err := client.Set(ctx, quotaLegacySweptKey, "", legacySweepInterval).Err(); err != nil {
		log.Errorf("failed to mark the legacy quota sweep done, error: %v", err)
	}
	if total > 0 {
		log.Infof("marked %d legacy quota keys as dirty", total)
	}
}
