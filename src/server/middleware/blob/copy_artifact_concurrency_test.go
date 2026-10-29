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

package blob

import (
	"context"
	"database/sql"
	"fmt"
	bo "github.com/beego/beego/v2/client/orm"
	db "github.com/goharbor/harbor/src/common/dao"
	ac "github.com/goharbor/harbor/src/controller/artifact"
	bc "github.com/goharbor/harbor/src/controller/blob"
	"github.com/goharbor/harbor/src/lib/orm"
	bd "github.com/goharbor/harbor/src/pkg/blob/dao"
	bm "github.com/goharbor/harbor/src/pkg/blob/models"
	pm "github.com/goharbor/harbor/src/pkg/project/models"
	at "github.com/goharbor/harbor/src/testing/controller/artifact"
	pt "github.com/goharbor/harbor/src/testing/controller/project"
	"github.com/goharbor/harbor/src/testing/mock"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type hookedTx struct {
	bo.TxOrmer
	afterBatch func()
}

func (h *hookedTx) Raw(q string, args ...any) bo.RawSeter {
	r := h.TxOrmer.Raw(q, args...)
	if strings.HasPrefix(q, "INSERT INTO project_blob") && h.afterBatch != nil {
		return &hookedRaw{RawSeter: r, h: h}
	}
	return r
}

type hookedRaw struct {
	bo.RawSeter
	h *hookedTx
}

func (r *hookedRaw) Exec() (sql.Result, error) {
	res, e := r.RawSeter.Exec()
	if e == nil && r.h.afterBatch != nil {
		f := r.h.afterBatch
		r.h.afterBatch = nil
		f()
	}
	return res, e
}
func waitForLock(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var b bool
		e := bo.NewOrm().Raw("SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=?", pid).QueryRow(&b)
		return e == nil && b
	}, 3*time.Second, time.Millisecond)
}

var dbOnce sync.Once

func TestCopyArtifactAndPutManifestConcurrently(t *testing.T) {
	dbOnce.Do(db.PrepareTestForPostgresSQL)
	oldA, oldB, oldP := artifactController, blobController, projectController
	defer func() { artifactController, blobController, projectController = oldA, oldB, oldP }()
	artifactCtl, projectCtl := &at.Controller{}, &pt.Controller{}
	artifactController, blobController, projectController = artifactCtl, bc.NewController(), projectCtl
	d := bd.New()
	ctx := orm.Context()
	cfg := digest.FromString("copy-test-config").String()
	layer := digest.FromString("copy-test-layer").String()
	body := fmt.Sprintf(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1,"digest":%q},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":1,"digest":%q}]}`, cfg, layer)
	manifest := digest.FromString(body).String()
	_, err := bo.NewOrm().Raw("TRUNCATE project_blob,artifact_blob,blob RESTART IDENTITY").Exec()
	require.NoError(t, err)
	// the manifest blob has the lowest id, so associating it separately after the references breaks the global order
	for _, b := range []*bm.Blob{{Digest: manifest, ContentType: "application/vnd.docker.distribution.manifest.v2+json", Size: int64(len(body))}, {Digest: cfg, ContentType: "application/vnd.docker.container.image.v1+json", Size: 1}, {Digest: layer, ContentType: "application/vnd.docker.image.rootfs.diff.tar.gzip", Size: 1}} {
		_, e := d.CreateBlob(ctx, b)
		require.NoError(t, e)
		_, e = d.CreateArtifactAndBlob(ctx, manifest, b.Digest)
		require.NoError(t, e)
	}
	art := &ac.Artifact{}
	art.ProjectID = 1
	art.Digest = manifest
	mock.OnAnything(artifactCtl, "GetByReference").Return(art, nil)
	mock.OnAnything(artifactCtl, "Walk").Return(nil).Run(func(args mock.Arguments) { require.NoError(t, args.Get(2).(func(*ac.Artifact) error)(art)) })
	mock.OnAnything(projectCtl, "GetByName").Return(&pm.Project{ProjectID: 810010}, nil)
	pushTx, err := bo.NewOrm().Begin()
	require.NoError(t, err)
	defer pushTx.Rollback()
	copyTx, err := bo.NewOrm().Begin()
	require.NoError(t, err)
	defer copyTx.Rollback()
	var copyPID, pushPID int
	require.NoError(t, copyTx.Raw("SELECT pg_backend_pid()").QueryRow(&copyPID))
	require.NoError(t, pushTx.Raw("SELECT pg_backend_pid()").QueryRow(&pushPID))
	copied := make(chan *httptest.ResponseRecorder, 1)
	hook := &hookedTx{TxOrmer: pushTx}
	hook.afterBatch = func() {
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/api/v2.0/projects/target/repositories/copy/artifacts?from=source/repo@"+manifest, nil).WithContext(orm.NewContext(context.Background(), copyTx))
			res := httptest.NewRecorder()
			CopyArtifactMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(201) })).ServeHTTP(res, req)
			if res.Code == 201 {
				err := copyTx.Commit()
				if err != nil {
					panic(err)
				}
			} else {
				copyTx.Rollback()
			}
			copied <- res
		}()
		waitForLock(t, copyPID)
	}
	req := httptest.NewRequest(http.MethodPut, "/v2/target/push/manifests/test", strings.NewReader(body)).WithContext(orm.NewContext(context.Background(), hook))
	req.Header.Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
	res := httptest.NewRecorder()
	PutManifestMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(201) })).ServeHTTP(res, req)
	if res.Code == 201 {
		require.NoError(t, pushTx.Commit())
	} else {
		pushTx.Rollback()
	}
	cr := <-copied
	require.Equal(t, http.StatusCreated, cr.Code, cr.Body.String())
	require.Equal(t, http.StatusCreated, res.Code, res.Body.String())
}

func TestCopyArtifactAssociatesBlobs(t *testing.T) {
	dbOnce.Do(db.PrepareTestForPostgresSQL)
	oldA, oldB, oldP := artifactController, blobController, projectController
	defer func() { artifactController, blobController, projectController = oldA, oldB, oldP }()
	artifactCtl, projectCtl := &at.Controller{}, &pt.Controller{}
	artifactController, blobController, projectController = artifactCtl, bc.NewController(), projectCtl
	d := bd.New()
	ctx := orm.Context()
	_, err := bo.NewOrm().Raw("TRUNCATE project_blob,artifact_blob,blob RESTART IDENTITY").Exec()
	require.NoError(t, err)
	arts := []*ac.Artifact{{}, {}, {}}
	for i, a := range arts {
		a.ProjectID = 1
		a.Digest = digest.FromString(fmt.Sprintf("copy-test-art-%d", i)).String()
	}
	ds := []string{arts[0].Digest, arts[1].Digest, arts[2].Digest, digest.FromString("copy-test-foreign").String(), digest.FromString("copy-test-shared").String(), digest.FromString("copy-test-unrelated").String()}
	ids := map[string]int64{}
	for _, dg := range ds {
		mt := "application/octet-stream"
		if dg == ds[3] {
			mt = "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip"
		}
		id, e := d.CreateBlob(ctx, &bm.Blob{Digest: dg, ContentType: mt, Size: 1})
		require.NoError(t, e)
		ids[dg] = id
	}
	for i, a := range arts {
		for _, dg := range []string{a.Digest, ds[4]} {
			_, e := d.CreateArtifactAndBlob(ctx, a.Digest, dg)
			require.NoError(t, e)
		}
		if i == 1 {
			_, e := d.CreateArtifactAndBlob(ctx, a.Digest, ds[3])
			require.NoError(t, e)
		}
	}
	preexisting, e := d.CreateProjectBlob(ctx, 810011, ids[ds[4]])
	require.NoError(t, e)
	mock.OnAnything(artifactCtl, "GetByReference").Return(arts[0], nil)
	mock.OnAnything(artifactCtl, "Walk").Return(nil).Run(func(args mock.Arguments) {
		require.True(t, args.Get(3).(*ac.Option).WithAccessory)
		for _, a := range arts {
			require.NoError(t, args.Get(2).(func(*ac.Artifact) error)(a))
		}
	})
	mock.OnAnything(projectCtl, "GetByName").Return(&pm.Project{ProjectID: 810011}, nil)
	for run := 0; run < 2; run++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v2.0/projects/target/repositories/copy/artifacts?from=source/repo@"+arts[0].Digest, nil).WithContext(ctx)
		res := httptest.NewRecorder()
		CopyArtifactMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(201) })).ServeHTTP(res, req)
		require.Equal(t, 201, res.Code)
	}
	var got []int64
	_, e = bo.NewOrm().Raw("SELECT blob_id FROM project_blob WHERE project_id=810011 ORDER BY blob_id").QueryRows(&got)
	require.NoError(t, e)
	require.Equal(t, []int64{1, 2, 3, 4, 5}, got)
	var same int64
	require.NoError(t, bo.NewOrm().Raw("SELECT id FROM project_blob WHERE project_id=810011 AND blob_id=?", ids[ds[4]]).QueryRow(&same))
	require.Equal(t, preexisting, same)
}
