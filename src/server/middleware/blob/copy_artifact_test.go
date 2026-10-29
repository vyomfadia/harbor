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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goharbor/harbor/src/controller/artifact"
	"github.com/goharbor/harbor/src/controller/blob"
	"github.com/goharbor/harbor/src/controller/project"
	blob_models "github.com/goharbor/harbor/src/pkg/blob/models"
	proModels "github.com/goharbor/harbor/src/pkg/project/models"
	artifacttesting "github.com/goharbor/harbor/src/testing/controller/artifact"
	blobtesting "github.com/goharbor/harbor/src/testing/controller/blob"
	projecttesting "github.com/goharbor/harbor/src/testing/controller/project"
	"github.com/goharbor/harbor/src/testing/mock"
)

func TestCopyArtifactMiddleware(t *testing.T) {
	defer func(a artifact.Controller, b blob.Controller, p project.Controller) {
		artifactController, blobController, projectController = a, b, p
	}(artifactController, blobController, projectController)

	artifactCtl := &artifacttesting.Controller{}
	blobCtl := &blobtesting.Controller{}
	projectCtl := &projecttesting.Controller{}
	artifactController, blobController, projectController = artifactCtl, blobCtl, projectCtl

	art := &artifact.Artifact{}
	art.ProjectID = 1
	art.Digest = "sha256:art"
	child := &artifact.Artifact{}
	child.ProjectID = 1
	child.Digest = "sha256:child"

	mock.OnAnything(artifactCtl, "GetByReference").Return(art, nil)
	mock.OnAnything(artifactCtl, "Walk").Return(nil).Run(func(args mock.Arguments) {
		walkFn := args.Get(2).(func(*artifact.Artifact) error)
		_ = walkFn(art)
		_ = walkFn(child)
	})
	mock.OnAnything(projectCtl, "GetByName").Return(&proModels.Project{ProjectID: 2}, nil)

	blobs := []*blob_models.Blob{{ID: 3, Digest: "sha256:c"}, {ID: 1, Digest: "sha256:a"}, {ID: 2, Digest: "sha256:b"}}
	mock.OnAnything(blobCtl, "List").Return(blobs, nil)
	mock.OnAnything(blobCtl, "FindMissingAssociationsForProject").Return(blobs, nil)
	// the blobs of all the artifacts are associated in one batch, which locks them in a consistent order
	blobCtl.On("AssociateWithProjectByDigests", mock.Anything, []string{"sha256:c", "sha256:a", "sha256:b"}, int64(2)).Return(nil).Once()

	req := httptest.NewRequest(http.MethodPost, "/api/v2.0/projects/target/repositories/hello/artifacts?from=source/hello@sha256:art", nil)
	res := httptest.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) })
	CopyArtifactMiddleware()(next).ServeHTTP(res, req)

	assert.Equal(t, http.StatusCreated, res.Code)
	blobCtl.AssertExpectations(t)
}
