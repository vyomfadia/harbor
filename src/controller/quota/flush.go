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
	// quotaKeyPrefix is the prefix of the keys holding the quotas updated in redis,
	// the "cache:" prefix keeps them consistent with the keys saved by cache.Default().
	quotaKeyPrefix = "cache:quota:"
	// quotaDirtySetKey is the redis set holding the keys of the quotas which have been
	// updated in redis but not flushed to db yet. Its name must not match the
	// "cache:*quota:*" pattern scanned by the previous versions.
	quotaDirtySetKey = "quota:dirty"
	// quotaFlushLockKey is the key of the lock which ensures only one instance flushes at a time.
	quotaFlushLockKey = "quota:flush_lock"
	// quotaFlushLockTTL is the ttl of the flush lock, the lock is extended periodically while flushing.
	quotaFlushLockTTL = time.Minute
	// quotaFlushBatchSize is the max number of keys popped from the dirty set at a time.
	quotaFlushBatchSize = 100
	// defaultQuotaFlushInterval is the default interval for flushing the quotas.
	defaultQuotaFlushInterval = 30 * time.Second
	// maxQuotaFlushInterval is the max interval for flushing the quotas, the quotas must
	// be flushed before they expire in redis, it is half of quotaExpireTimeout.
	maxQuotaFlushInterval = 150 * time.Second
)

var (
	// releaseLockScript deletes the lock only when it is still held by the given token.
	releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)
	// extendLockScript extends the ttl of the lock only when it is still held by the given token.
	extendLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`)
)

// quotaFlushInterval returns the interval for flushing the quotas from redis to db.
func quotaFlushInterval() time.Duration {
	seconds := config.GetQuotaFlushIntervalSeconds()
	if seconds <= 0 {
		log.Warningf("invalid quota flush interval %d seconds, use the default value %s", seconds, defaultQuotaFlushInterval)
		return defaultQuotaFlushInterval
	}

	interval := time.Duration(seconds) * time.Second
	if interval > maxQuotaFlushInterval {
		log.Warningf("quota flush interval %s exceeds the max value, use the max value %s", interval, maxQuotaFlushInterval)
		return maxQuotaFlushInterval
	}

	return interval
}

// quotaFlusher flushes the quotas updated in redis to db.
type quotaFlusher struct {
	getClient func() (*redis.Client, error)
	update    func(ctx context.Context, q *quota.Quota) error
	lockTTL   time.Duration

	// legacySweepAt is the time after which the one-off sweep of the legacy quota keys runs.
	legacySweepAt time.Time
	legacySwept   bool
}

func newQuotaFlusher() *quotaFlusher {
	return &quotaFlusher{
		getClient: libredis.GetHarborClient,
		update: func(ctx context.Context, q *quota.Quota) error {
			return Ctl.Update(ctx, q)
		},
		lockTTL: quotaFlushLockTTL,
		// run the sweep after the instances of the previous version are expected to be gone
		// during a rolling upgrade, and before the quota keys written by them expire, the
		// delay plus maxQuotaFlushInterval is less than quotaExpireTimeout.
		legacySweepAt: time.Now().Add(2 * time.Minute),
	}
}

// flush flushes the quotas recorded in the dirty set to db. Only the instance which
// holds the flush lock does the work, the others return immediately.
func (f *quotaFlusher) flush(ctx context.Context) {
	client, err := f.getClient()
	if err != nil {
		log.Errorf("failed to get the redis client for flushing quota, error: %v", err)
		return
	}

	f.sweepLegacyKeys(ctx, client)

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
	flushed, failed := f.drain(ctx, client, lost)
	metric.QuotaFlushDurSummary.Observe(time.Since(start).Seconds())
	metric.QuotaFlushKeysTotal.WithLabelValues("flushed").Add(float64(flushed))
	metric.QuotaFlushKeysTotal.WithLabelValues("failed").Add(float64(failed))
	if flushed+failed > 0 {
		log.Debugf("flushed quotas from redis to db, %d flushed, %d failed, took %s", flushed, failed, time.Since(start))
	}
}

// drain pops the keys from the dirty set and flushes them to db. The keys which fail
// to be flushed are put back to the dirty set to be retried in the next round.
//
// A key is removed from the dirty set before its value is read, so an update which
// happens during the flush always marks the key dirty again and the key is flushed
// again in the next round, no update is lost.
func (f *quotaFlusher) drain(ctx context.Context, client *redis.Client, lost *atomic.Bool) (flushed, failed int) {
	// only handle the keys which are dirty when the round starts, the keys marked
	// dirty again during this round are handled in the next round.
	pending, err := client.SCard(ctx, quotaDirtySetKey).Result()
	if err != nil {
		log.Errorf("failed to get the size of the quota dirty set, error: %v", err)
		return
	}

	for pending > 0 && !lost.Load() {
		keys, err := client.SPopN(ctx, quotaDirtySetKey, min(pending, quotaFlushBatchSize)).Result()
		if err != nil {
			log.Errorf("failed to pop keys from the quota dirty set, error: %v", err)
			return
		}
		if len(keys) == 0 {
			return
		}
		pending -= int64(len(keys))

		var retries []any
		for i, key := range keys {
			if lost.Load() {
				// the lock is lost, give the remaining keys back for the next holder
				for _, k := range keys[i:] {
					retries = append(retries, k)
				}
				break
			}
			if err := f.flushKey(ctx, client, key); err != nil {
				log.Errorf("failed to flush quota %s, error: %v", key, err)
				retries = append(retries, key)
				failed++
				continue
			}
			flushed++
		}

		if len(retries) > 0 {
			if err := client.SAdd(context.WithoutCancel(ctx), quotaDirtySetKey, retries...).Err(); err != nil {
				log.Errorf("failed to put %d keys back to the quota dirty set, error: %v", len(retries), err)
			}
		}
	}

	return
}

// flushKey flushes the quota stored in the key to db.
func (f *quotaFlusher) flushKey(ctx context.Context, client *redis.Client, key string) error {
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

// keepLock extends the flush lock periodically until ctx is done, the returned flag
// is set once the lock can not be extended, which means it may be held by others.
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

// sweepLegacyKeys marks all the quota keys in redis as dirty once per process.
// The previous versions flushed the quotas by scanning the whole keyspace and did
// not track the dirty keys, so the quotas they updated right before they were
// replaced in a rolling upgrade would not be flushed without this sweep.
func (f *quotaFlusher) sweepLegacyKeys(ctx context.Context, client *redis.Client) {
	if f.legacySwept || time.Now().Before(f.legacySweepAt) {
		return
	}
	f.legacySwept = true

	var keys []any
	total := 0
	addKeys := func() {
		if len(keys) == 0 {
			return
		}
		if err := client.SAdd(ctx, quotaDirtySetKey, keys...).Err(); err != nil {
			log.Errorf("failed to add legacy quota keys to the dirty set, error: %v", err)
		}
		total += len(keys)
		keys = keys[:0]
	}

	iter := client.Scan(ctx, 0, quotaKeyPrefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 1000 {
			addKeys()
		}
	}
	addKeys()
	if err := iter.Err(); err != nil {
		log.Errorf("failed to scan the legacy quota keys, error: %v", err)
	}

	if total > 0 {
		log.Infof("marked %d legacy quota keys as dirty", total)
	}
}
