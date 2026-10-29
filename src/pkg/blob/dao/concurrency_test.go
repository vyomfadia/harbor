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
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/pkg/blob/models"
)

// associateFunc associates the blobs with the project inside the transaction carried by ctx,
// the step is called between the statements to emulate the non-database work of a request
type associateFunc func(ctx context.Context, projectID int64, digests []string, step func()) error

type pushResult struct {
	deadlocks int
	errs      []error
}

// simulatePushes emulates concurrent manifest pushes sharing blobs: every push associates
// a random subset of the shared blobs in a random order with the same project, and then
// associates its own manifest blob, all in one transaction just like the request
// transaction of the registry API. A part of the shared blobs is unassociated before
// every round so that the pushes contend on both existing and new associations.
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
