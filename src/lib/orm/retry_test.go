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

package orm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/metric"
	ormtesting "github.com/goharbor/harbor/src/testing/lib/orm"
)

func shortenTxRetry(t *testing.T, attempts int) {
	maxAttempts, backoff, maxWait := txMaxAttempts, txRetryBackoff, txRetryMaxWait
	txMaxAttempts, txRetryBackoff, txRetryMaxWait = attempts, time.Millisecond, 2*time.Millisecond
	t.Cleanup(func() {
		txMaxAttempts, txRetryBackoff, txRetryMaxWait = maxAttempts, backoff, maxWait
	})
}

func counterValue(t *testing.T, c prometheus.Counter) float64 {
	m := &dto.Metric{}
	assert.NoError(t, c.Write(m))
	return m.GetCounter().GetValue()
}

func deadlockErr() error {
	return &pgconn.PgError{
		Code:    "40P01",
		Message: "deadlock detected",
		Detail:  "Process 1 waits for ShareLock on transaction 2; blocked by process 3.",
		Where:   `while inserting index tuple (0,1) in relation "project_blob"`,
	}
}

func TestIsRetryableTxError(t *testing.T) {
	assert := assert.New(t)
	assert.True(IsRetryableTxError(deadlockErr()))
	assert.True(IsRetryableTxError(&pgconn.PgError{Code: "40001"}))
	assert.True(IsRetryableTxError(errors.Wrap(deadlockErr(), "wrapped")))
	assert.True(IsRetryableTxError(fmt.Errorf("wrapped: %w", deadlockErr())))
	assert.False(IsRetryableTxError(&pgconn.PgError{Code: "23505"}))
	assert.False(IsRetryableTxError(errors.New("deadlock detected")))
	assert.False(IsRetryableTxError(nil))
}

func TestRetryTransactionRetriesDeadlock(t *testing.T) {
	shortenTxRetry(t, 4)
	ctx := SetTransactionOpNameToContext(context.Background(), "tx-test-retry-deadlock")
	retries := metric.TotalDBDeadlockRetries.WithLabelValues("tx-test-retry-deadlock", "40P01")
	giveUps := metric.TotalDBDeadlockGiveUps.WithLabelValues("tx-test-retry-deadlock", "40P01")

	calls := 0
	err := RetryTransaction(ctx, func() error {
		calls++
		if calls < 3 {
			return errors.Wrap(deadlockErr(), "associate blob with project failed")
		}
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, calls)
	assert.Equal(t, float64(2), counterValue(t, retries))
	assert.Equal(t, float64(0), counterValue(t, giveUps))
}

func TestRetryTransactionNotRetryOtherErrors(t *testing.T) {
	shortenTxRetry(t, 4)
	for _, e := range []error{errors.New("boom"), &pgconn.PgError{Code: "23505"}} {
		calls := 0
		err := RetryTransaction(context.Background(), func() error {
			calls++
			return e
		})
		assert.Equal(t, e, err)
		assert.Equal(t, 1, calls)
	}
}

func TestRetryTransactionMaxAttempts(t *testing.T) {
	shortenTxRetry(t, 3)
	ctx := SetTransactionOpNameToContext(context.Background(), "tx-test-retry-giveup")
	retries := metric.TotalDBDeadlockRetries.WithLabelValues("tx-test-retry-giveup", "40001")
	giveUps := metric.TotalDBDeadlockGiveUps.WithLabelValues("tx-test-retry-giveup", "40001")

	calls := 0
	err := RetryTransaction(ctx, func() error {
		calls++
		return &pgconn.PgError{Code: "40001"}
	})
	assert.True(t, IsRetryableTxError(err))
	assert.Equal(t, 3, calls)
	assert.Equal(t, float64(2), counterValue(t, retries))
	assert.Equal(t, float64(1), counterValue(t, giveUps))
}

func TestRetryTransactionContextCanceled(t *testing.T) {
	shortenTxRetry(t, 5)
	txRetryBackoff, txRetryMaxWait = time.Hour, time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	done := make(chan error)
	go func() {
		done <- RetryTransaction(ctx, func() error {
			calls++
			return deadlockErr()
		})
	}()
	cancel()

	select {
	case err := <-done:
		assert.True(t, IsRetryableTxError(err))
		assert.Equal(t, 1, calls)
	case <-time.After(10 * time.Second):
		t.Fatal("the retry doesn't stop when the context is canceled")
	}
}

func TestRetryTransactionInEnclosingTransaction(t *testing.T) {
	shortenTxRetry(t, 4)
	ctx := NewContext(context.Background(), &ormtesting.FakeTxOrmer{})
	calls := 0
	err := RetryTransaction(ctx, func() error {
		calls++
		return deadlockErr()
	})
	assert.True(t, IsRetryableTxError(err))
	assert.Equal(t, 1, calls)
}

func TestWithRetryableTransaction(t *testing.T) {
	shortenTxRetry(t, 4)
	ctx := NewContext(context.Background(), &ormtesting.FakeOrmer{})

	calls := 0
	err := WithRetryableTransaction(func(ctx context.Context) error {
		calls++
		// every attempt runs in a transaction which isn't cached
		o, err := FromContext(ctx)
		assert.NoError(t, err)
		assert.IsType(t, &ormtesting.FakeTxOrmer{}, o)
		assert.True(t, HasCommittedKey(ctx))
		if calls == 1 {
			return deadlockErr()
		}
		return nil
	})(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestTxRetryDelay(t *testing.T) {
	maxWait := txRetryMaxWait
	for attempt := 1; attempt < 70; attempt++ {
		d := txRetryDelay(attempt)
		assert.Positive(t, d)
		assert.LessOrEqual(t, d, maxWait)
	}
	// grows with the attempts
	assert.LessOrEqual(t, txRetryDelay(1), txRetryBackoff)
	assert.GreaterOrEqual(t, txRetryDelay(3), 2*txRetryBackoff)
}

func TestDescribeTxError(t *testing.T) {
	s := describeTxError(errors.Wrap(deadlockErr(), "wrapped"))
	assert.Contains(t, s, "sqlstate=40P01")
	assert.Contains(t, s, "Process 1 waits for ShareLock")
	assert.Contains(t, s, `relation \"project_blob\"`)
	assert.Equal(t, "boom", describeTxError(errors.New("boom")))
}
