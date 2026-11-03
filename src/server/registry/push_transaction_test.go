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

package registry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	beego "github.com/beego/beego/v2/client/orm"
	beegocontext "github.com/beego/beego/v2/server/web/context"
	"github.com/docker/distribution"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/common"
	"github.com/goharbor/harbor/src/controller/artifact/processor"
	"github.com/goharbor/harbor/src/controller/artifact/processor/base"
	quotaCtl "github.com/goharbor/harbor/src/controller/quota"
	"github.com/goharbor/harbor/src/lib"
	"github.com/goharbor/harbor/src/lib/cache"
	"github.com/goharbor/harbor/src/lib/config"
	libhttp "github.com/goharbor/harbor/src/lib/http"
	"github.com/goharbor/harbor/src/lib/orm"
	libredis "github.com/goharbor/harbor/src/lib/redis"
	_ "github.com/goharbor/harbor/src/pkg/config/inmemory"
	"github.com/goharbor/harbor/src/pkg/notification"
	pkgquota "github.com/goharbor/harbor/src/pkg/quota"
	"github.com/goharbor/harbor/src/pkg/quota/types"
	registryclient "github.com/goharbor/harbor/src/pkg/registry"
	"github.com/goharbor/harbor/src/server/middleware"
	"github.com/goharbor/harbor/src/server/middleware/artifactinfo"
	"github.com/goharbor/harbor/src/server/middleware/blob"
	"github.com/goharbor/harbor/src/server/middleware/cosign"
	"github.com/goharbor/harbor/src/server/middleware/quota"
	"github.com/goharbor/harbor/src/server/middleware/subject"
	"github.com/goharbor/harbor/src/server/middleware/transaction"
	"github.com/goharbor/harbor/src/server/router"
	htesting "github.com/goharbor/harbor/src/testing"
)

type pushRegistry struct {
	registryclient.Client
	manifest                   distribution.Manifest
	config                     []byte
	manifestReads, blobReads   atomic.Int32
	beforeManifest, beforeBlob func()
	blobError                  error
}

func (r *pushRegistry) PullManifest(string, string, ...string) (distribution.Manifest, string, error) {
	r.manifestReads.Add(1)
	if r.beforeManifest != nil {
		r.beforeManifest()
	}
	return r.manifest, "", nil
}
func (r *pushRegistry) PullBlob(string, string) (int64, io.ReadCloser, error) {
	r.blobReads.Add(1)
	if r.beforeBlob != nil {
		r.beforeBlob()
	}
	if r.blobError != nil {
		return 0, nil, r.blobError
	}
	return int64(len(r.config)), io.NopCloser(bytes.NewReader(r.config)), nil
}

// Exercises the actual persistence and Redis quota controllers with one database
// connection. A registry stall must not prevent unrelated SQL, and retry/rollback
// must leave artifact, tag, blob associations, quota and events consistent.
func TestPushTransactionIntegrity(t *testing.T) {
	if os.Getenv("POSTGRESQL_HOST") == "" || os.Getenv("_REDIS_URL_HARBOR") == "" {
		t.Skip("requires isolated PostgreSQL and Redis test services")
	}
	t.Setenv("QUOTA_UPDATE_PROVIDER", "redis")
	s := &htesting.Suite{}
	s.SetT(t)
	s.SetupSuite()
	oldConfig := config.DefaultCfgManager
	defer func() { config.DefaultCfgManager = oldConfig }()
	config.InitWithSettings(map[string]any{common.QuotaUpdateProvider: "redis"})
	require.Equal(t, "redis", config.GetQuotaUpdateProvider())
	db, err := beego.GetDB()
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	defer db.SetMaxOpenConns(100)
	oldProxy, oldClient := proxy, registryclient.Cli
	oldProcessor := processor.Registry[v1.MediaTypeImageConfig]
	defer func() {
		proxy = oldProxy
		registryclient.Cli = oldClient
		processor.Registry[v1.MediaTypeImageConfig] = oldProcessor
	}()

	for _, outcome := range []string{"success", "retry", "rollback", "commit-error", "cancelled", "metadata-error", "registry-error"} {
		t.Run(outcome, func(t *testing.T) {
			s.SetT(t)
			s.WithProject(func(projectID int64, projectName string) {
				ctx := s.Context()
				requestCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				_, err := quotaCtl.Ctl.Create(ctx, "project", fmt.Sprint(projectID), types.ResourceList{types.ResourceStorage: 1 << 20})
				require.NoError(t, err)
				cfg := []byte(`{"architecture":"amd64"}`)
				body := []byte(fmt.Sprintf(`{"schemaVersion":2,"mediaType":%q,"config":{"mediaType":%q,"digest":%q,"size":%d},"layers":[],"subject":{"mediaType":%q,"digest":%q,"size":1}}`, v1.MediaTypeImageManifest, v1.MediaTypeImageConfig, digest.FromBytes(cfg), len(cfg), v1.MediaTypeImageManifest, digest.FromString("subject")))
				mf, _, err := distribution.UnmarshalManifest(v1.MediaTypeImageManifest, body)
				require.NoError(t, err)
				backend := &pushRegistry{manifest: mf, config: cfg}
				if outcome == "metadata-error" {
					backend.blobError = errors.New("injected config fetch failure")
				}
				registryclient.Cli = backend
				processor.Registry[v1.MediaTypeImageConfig] = base.NewManifestProcessor()
				var puts atomic.Int32
				// Query through the same one-connection pool while network work is stalled.
				// If a transaction spans this callback it would time out waiting for its own connection.
				checkPool := func() {
					checkCtx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					var one int
					if err := db.QueryRowContext(checkCtx, "SELECT 1").Scan(&one); err != nil {
						t.Errorf("registry I/O pinned the only connection: %v", err)
					}
				}
				backend.beforeManifest = checkPool
				backend.beforeBlob = func() {
					checkPool()
					if outcome == "cancelled" {
						cancel()
					}
				}
				proxy = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					puts.Add(1)
					checkPool()
					if outcome == "registry-error" {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
					w.WriteHeader(http.StatusCreated)
				})
				attempts := 0
				inject := middleware.New(func(w http.ResponseWriter, r *http.Request, next http.Handler) {
					attempts++
					next.ServeHTTP(w, r)
					if outcome == "rollback" || (outcome == "retry" && attempts == 1) {
						buffer := w.(*lib.ResponseBuffer)
						require.NoError(t, buffer.Reset())
						libhttp.SendError(w, &pgconn.PgError{Code: "40001", Message: "injected serialization failure after persistence"})
					}
				})
				h := middleware.Chain(artifactinfo.Middleware(), quota.PutManifestMiddleware(), blob.PutManifestProbeMiddleware(), proxyManifest(), prepareManifest(), transaction.RetryableMiddleware("test-push"), inject, cosign.SignatureMiddleware(), subject.Middleware(), blob.PutManifestSyncMiddleware())(http.HandlerFunc(putManifest))
				repo := projectName + "/test"
				req := httptest.NewRequest(http.MethodPut, "/v2/"+repo+"/manifests/latest", bytes.NewReader(body))
				req.Header.Set("Content-Type", v1.MediaTypeImageManifest)
				input := &beegocontext.BeegoInput{}
				input.SetParam(":splat", repo)
				input.SetParam(":reference", "latest")
				events := notification.NewEventCtx()
				if outcome == "commit-error" {
					requestCtx = orm.NewContext(requestCtx, &failedCommitOrmer{Ormer: beego.NewOrm()})
				}
				req = req.WithContext(notification.NewContext(context.WithValue(requestCtx, router.ContextKeyInput{}, input), events))
				response := httptest.NewRecorder()
				h.ServeHTTP(response, req)
				success := outcome == "success" || outcome == "retry"
				if success {
					require.Equal(t, 201, response.Code, response.Body.String())
				} else if outcome == "registry-error" {
					require.Equal(t, 400, response.Code)
				} else {
					require.Equal(t, 500, response.Code, response.Body.String())
				}
				networkCalls := 1
				if outcome == "success" {
					// A client can retry a successful PUT after losing the response.
					// The second push must not create another row or charge quota again.
					replay := req.Clone(req.Context())
					replay.Body = io.NopCloser(bytes.NewReader(body))
					replayResponse := httptest.NewRecorder()
					h.ServeHTTP(replayResponse, replay)
					require.Equal(t, http.StatusCreated, replayResponse.Code, replayResponse.Body.String())
					networkCalls = 2
				}
				require.EqualValues(t, networkCalls, puts.Load())
				if outcome != "registry-error" {
					require.EqualValues(t, networkCalls, backend.manifestReads.Load())
					require.EqualValues(t, networkCalls, backend.blobReads.Load())
				}
				if outcome == "retry" {
					require.Equal(t, 2, attempts)
				}
				expected := 0
				if success {
					expected = 1
				}
				for _, query := range []string{
					"SELECT count(*) FROM artifact WHERE repository_name = $1",
					"SELECT count(*) FROM artifact_accessory WHERE subject_artifact_repo = $1",
					"SELECT count(*) FROM tag t JOIN repository r ON t.repository_id=r.repository_id WHERE r.name=$1",
				} {
					var count int
					require.NoError(t, db.QueryRow(query, repo).Scan(&count))
					require.Equal(t, expected, count, query)
				}
				var associated int
				require.NoError(t, db.QueryRow("SELECT count(*) FROM project_blob WHERE project_id=$1", projectID).Scan(&associated))
				require.Equal(t, expected*2, associated)
				require.Equal(t, expected*networkCalls, events.Events.Len())
				// Read the live Redis quota, not the asynchronously flushed DB copy.
				rc, err := libredis.GetHarborClient()
				require.NoError(t, err)
				key := fmt.Sprintf("cache:quota:project:%d", projectID)
				raw, err := rc.Get(ctx, key).Bytes()
				require.NoError(t, err)
				q := &pkgquota.Quota{}
				require.NoError(t, cache.DefaultCodec().Decode(raw, q))
				used, err := q.GetUsed()
				require.NoError(t, err)
				require.EqualValues(t, expected*(len(body)+len(cfg)), used[types.ResourceStorage])
				require.NoError(t, rc.Del(ctx, key).Err())
			})
		})
	}
}

type failedCommitOrmer struct{ beego.Ormer }

func (o *failedCommitOrmer) Begin() (beego.TxOrmer, error) {
	tx, err := o.Ormer.Begin()
	if err != nil {
		return nil, err
	}
	return &failedCommitTx{TxOrmer: tx}, nil
}

type failedCommitTx struct{ beego.TxOrmer }

func (tx *failedCommitTx) Commit() error {
	if err := tx.TxOrmer.Rollback(); err != nil {
		return err
	}
	return errors.New("injected failed commit")
}
