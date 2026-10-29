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
	"math/rand"
	"strings"
	"sync"
	"time"

	beegoorm "github.com/beego/beego/v2/client/orm"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/pkg/blob/models"
)

// associateFunc associates the blobs with the project, step emulates the non-database work of a request
type associateFunc func(ctx context.Context, projectID int64, digests []string, step func()) error

type pushResult struct {
	deadlocks int
	errs      []error
}

// simulatePushes emulates concurrent manifest pushes sharing blobs: each push associates a random
// subset of the shared blobs in random order, then its own manifest blob, in one transaction.
// Some shared blobs are unassociated before each round to contend on both existing and new rows.
func (suite *DaoTestSuite) simulatePushes(associate associateFunc, workers, rounds, shared, fresh int) pushResult {
	ctx := suite.Context()
	projectID := int64(rand.Int31())

	var digests []string
	var freshIDs []int64
	for i := range shared {
		digest := suite.DigestString()
		blobID, err := suite.dao.CreateBlob(ctx, &models.Blob{Digest: digest})
		suite.Require().NoError(err)
		digests = append(digests, digest)
		if i < fresh {
			freshIDs = append(freshIDs, blobID)
		} else {
			_, err = suite.dao.CreateProjectBlob(ctx, projectID, blobID)
			suite.Require().NoError(err)
		}
	}

	var (
		mu     sync.Mutex
		result pushResult
	)
	for range rounds {
		suite.Require().NoError(suite.dao.DeleteProjectBlob(ctx, projectID, freshIDs...))

		var wg sync.WaitGroup
		start := make(chan struct{})
		for range workers {
			manifest := suite.DigestString()
			manifestID, err := suite.dao.CreateBlob(ctx, &models.Blob{Digest: manifest})
			suite.Require().NoError(err)

			wg.Add(1)
			go func() {
				defer wg.Done()

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				refs := make([]string, len(digests))
				copy(refs, digests)
				r.Shuffle(len(refs), func(i, j int) { refs[i], refs[j] = refs[j], refs[i] })
				refs = refs[:len(refs)-r.Intn(len(refs)/4+1)]
				step := func() { time.Sleep(time.Duration(r.Intn(2000)) * time.Microsecond) }

				<-start
				err := orm.WithTransaction(func(ctx context.Context) error {
					if err := associate(ctx, projectID, refs, step); err != nil {
						return err
					}
					step()
					if _, err := suite.dao.CreateProjectBlob(ctx, projectID, manifestID); err != nil {
						return err
					}
					step()
					return nil
				})(suite.Context())

				if err != nil {
					mu.Lock()
					defer mu.Unlock()
					var pgErr *pgconn.PgError
					if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
						result.deadlocks++
					} else {
						result.errs = append(result.errs, err)
					}
				}
			}()
		}
		close(start)
		wg.Wait()
	}

	return result
}

func (suite *DaoTestSuite) TestAssociateBlobsWithProjectConcurrently() {
	associate := func(ctx context.Context, projectID int64, digests []string, step func()) error {
		step()
		return suite.dao.AssociateBlobsWithProject(ctx, projectID, digests)
	}

	result := suite.simulatePushes(associate, 32, 5, 16, 4)
	suite.Zero(result.deadlocks)
	suite.Empty(result.errs)
}

// conflictHook calls onConflict when an insert into table hits an existing row
type conflictHook struct {
	beegoorm.QueryExecutor
	table      string
	onConflict func()
}

func (h *conflictHook) Raw(query string, args ...any) beegoorm.RawSeter {
	raw := h.QueryExecutor.Raw(query, args...)
	if !strings.HasPrefix(query, "INSERT INTO "+h.table+" ") {
		return raw
	}
	return &conflictRaw{RawSeter: raw, hook: h}
}

type conflictRaw struct {
	beegoorm.RawSeter
	hook *conflictHook
}

func (r *conflictRaw) QueryRow(containers ...any) error {
	err := r.RawSeter.QueryRow(containers...)
	if errors.Is(err, orm.ErrNoRows) && r.hook.onConflict != nil {
		r.hook.onConflict()
	}
	return err
}

// withDeleteAfterConflict returns a context whose queries run in a transaction where the first
// insert conflict into table is followed by del, committed from another session
func (suite *DaoTestSuite) withDeleteAfterConflict(table string, del func(ctx context.Context) error, f func(ctx context.Context)) {
	tx, err := beegoorm.NewOrm().Begin()
	suite.Require().NoError(err)
	defer tx.Rollback()

	hook := &conflictHook{QueryExecutor: tx, table: table}
	hook.onConflict = func() {
		hook.onConflict = nil
		suite.Require().NoError(del(suite.Context()))
	}
	f(orm.NewContext(context.TODO(), hook))
}

func (suite *DaoTestSuite) TestCreateProjectBlobConcurrentDelete() {
	ctx := suite.Context()
	projectID := int64(1)
	blobID, err := suite.dao.CreateBlob(ctx, &models.Blob{Digest: suite.DigestString()})
	suite.Require().NoError(err)
	_, err = suite.dao.CreateProjectBlob(ctx, projectID, blobID)
	suite.Require().NoError(err)

	del := func(ctx context.Context) error { return suite.dao.DeleteProjectBlob(ctx, projectID, blobID) }
	suite.withDeleteAfterConflict("project_blob", del, func(ctx context.Context) {
		id, err := suite.dao.CreateProjectBlob(ctx, projectID, blobID)
		suite.Require().NoError(err)
		suite.NotZero(id)
	})
}

func (suite *DaoTestSuite) TestCreateArtifactAndBlobConcurrentDelete() {
	ctx := suite.Context()
	artifactDigest := suite.DigestString()
	blobDigest := suite.DigestString()
	_, err := suite.dao.CreateArtifactAndBlob(ctx, artifactDigest, blobDigest)
	suite.Require().NoError(err)

	del := func(ctx context.Context) error { return suite.dao.DeleteArtifactAndBlobByArtifact(ctx, artifactDigest) }
	suite.withDeleteAfterConflict("artifact_blob", del, func(ctx context.Context) {
		id, err := suite.dao.CreateArtifactAndBlob(ctx, artifactDigest, blobDigest)
		suite.Require().NoError(err)
		suite.NotZero(id)
	})
}
