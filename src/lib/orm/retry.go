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
	"math/rand/v2"
	"strings"
	"time"

	"github.com/beego/beego/v2/client/orm"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/metric"
)

// variables so that the tests can shorten them
var (
	txMaxAttempts  = 4
	txRetryBackoff = 20 * time.Millisecond
	txRetryMaxWait = 500 * time.Millisecond
)

// RetryTransaction calls run, which must execute one complete transaction, again with a
// jittered backoff while PostgreSQL aborts it with a deadlock or a serialization failure,
// up to a bounded number of attempts. run may execute several times, so it must only
// have side effects that can be repeated.
//
// When ctx already carries a transaction, run is called once: it would be a savepoint of
// the enclosing transaction whose locks are still held, so retrying it would most likely
// deadlock again. Retrying is left to the owner of the enclosing transaction.
func RetryTransaction(ctx context.Context, run func() error) error {
	if inTransaction(ctx) {
		return run()
	}

	for attempt := 1; ; attempt++ {
		err := run()
		if err == nil || !IsRetryableTxError(err) {
			return err
		}

		op := GetTransactionOpNameFromContext(ctx)
		sqlState := pgErrorCode(err)
		logger := log.G(ctx)
		if attempt >= txMaxAttempts {
			metric.TotalDBDeadlockGiveUps.WithLabelValues(op, sqlState).Inc()
			logger.Errorf("transaction %s aborted by the database on attempt %d/%d, giving up: %s",
				op, attempt, txMaxAttempts, describeTxError(err))
			return err
		}

		wait := txRetryDelay(attempt)
		metric.TotalDBDeadlockRetries.WithLabelValues(op, sqlState).Inc()
		logger.Warningf("transaction %s aborted by the database on attempt %d/%d, retrying in %s: %s",
			op, attempt, txMaxAttempts, wait, describeTxError(err))

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// WithRetryableTransaction is like WithTransaction, but runs f again in a new transaction
// when it is aborted by a deadlock or a serialization failure, see RetryTransaction
func WithRetryableTransaction(f func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if !inTransaction(ctx) {
			// keep the cached managers from caching data of an attempt that may be rolled back
			ctx = context.WithValue(ctx, CommittedKey{}, new(bool))
		}
		return RetryTransaction(ctx, func() error {
			return WithTransaction(f)(ctx)
		})
	}
}

func inTransaction(ctx context.Context) bool {
	o, err := FromContext(ctx)
	if err != nil {
		return false
	}
	_, ok := o.(orm.TxOrmer)
	return ok
}

// txRetryDelay returns the exponential backoff capped by txRetryMaxWait with "equal jitter",
// so the transactions that deadlocked each other don't retry in lockstep
func txRetryDelay(attempt int) time.Duration {
	d := txRetryBackoff << (attempt - 1)
	if d <= 0 || d > txRetryMaxWait {
		d = txRetryMaxWait
	}
	half := d / 2
	return half + rand.N(half+1)
}

func pgErrorCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// describeTxError returns the details PostgreSQL reports for the error
func describeTxError(err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err.Error()
	}

	parts := []string{fmt.Sprintf("sqlstate=%s message=%q", pgErr.Code, pgErr.Message)}
	for _, f := range []struct{ k, v string }{
		{"detail", pgErr.Detail},
		{"where", pgErr.Where},
		{"table", pgErr.TableName},
		{"constraint", pgErr.ConstraintName},
		{"query", pgErr.InternalQuery},
	} {
		if f.v != "" {
			parts = append(parts, fmt.Sprintf("%s=%q", f.k, f.v))
		}
	}
	return strings.Join(parts, " ")
}
