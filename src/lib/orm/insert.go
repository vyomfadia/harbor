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
	"time"

	"github.com/goharbor/harbor/src/lib/errors"
)

// InsertOrGetID runs insert, an "INSERT ... ON CONFLICT DO NOTHING RETURNING id", and when it inserts
// nothing runs query for the id of the existing row. Unlike a plain insert, a duplicate doesn't fail
// the statement, so the surrounding transaction stays usable without a savepoint. Both are retried
// as the existing row can be deleted by others in between.
func InsertOrGetID(ctx context.Context, insert string, insertArgs []any, query string, queryArgs ...any) (id int64, inserted bool, err error) {
	o, err := FromContext(ctx)
	if err != nil {
		return 0, false, err
	}
	for range 3 {
		err = o.Raw(insert, insertArgs...).QueryRow(&id)
		if !errors.Is(err, ErrNoRows) {
			return id, err == nil, err
		}
		err = o.Raw(query, queryArgs...).QueryRow(&id)
		if !errors.Is(err, ErrNoRows) {
			return id, false, err
		}
	}
	return 0, false, errors.ConflictError(nil).WithMessage("the row is being deleted and recreated concurrently, retry later")
}

// TimeArg converts t to a raw query argument to be cast with "?::timestamptz", so that a timestamp column
// stores it the same as by Insert: NULL if zero, otherwise in the database timezone with microseconds,
// while raw queries format time.Time to seconds.
func TimeArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.Format("2006-01-02 15:04:05.999999-07:00")
}
