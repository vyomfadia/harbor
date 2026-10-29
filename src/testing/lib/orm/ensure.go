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
	"sync"
	"time"

	liborm "github.com/goharbor/harbor/src/lib/orm"
)

// EnsureResult collects the results of EnsureConcurrently
type EnsureResult struct {
	IDs     []int64
	Created int
	Errs    []error
}

// EnsureConcurrently calls ensure from n goroutines at once, each in its own transaction. The transactions
// stay open for a while after ensure so that the others contend with an uncommitted row.
func EnsureConcurrently(ctx context.Context, n int, ensure func(ctx context.Context) (bool, int64, error)) EnsureResult {
	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		result EnsureResult
	)
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var (
				created bool
				id      int64
			)
			err := liborm.WithTransaction(func(ctx context.Context) (err error) {
				created, id, err = ensure(ctx)
				time.Sleep(50 * time.Millisecond)
				return err
			})(ctx)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				result.Errs = append(result.Errs, err)
				return
			}
			result.IDs = append(result.IDs, id)
			if created {
				result.Created++
			}
		}()
	}
	close(start)
	wg.Wait()
	return result
}

// RowJSON returns the row of table whose idColumn is id as JSON, without the omitted columns
func RowJSON(ctx context.Context, table, idColumn string, id int64, omit ...string) (string, error) {
	o, err := liborm.FromContext(ctx)
	if err != nil {
		return "", err
	}
	query := "SELECT (to_jsonb(t)"
	for _, col := range omit {
		query += fmt.Sprintf(" - '%s'", col)
	}
	query += fmt.Sprintf(")::text FROM %s t WHERE %s = ?", table, idColumn)
	var row string
	err = o.Raw(query, id).QueryRow(&row)
	return row, err
}
