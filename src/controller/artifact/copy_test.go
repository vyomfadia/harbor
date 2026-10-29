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

package artifact

import (
	"context"
	"time"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/mock"

	commondao "github.com/goharbor/harbor/src/common/dao"
	"github.com/goharbor/harbor/src/controller/tag"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/q"
	"github.com/goharbor/harbor/src/pkg/accessory"
	accessorymodel "github.com/goharbor/harbor/src/pkg/accessory/model"
	"github.com/goharbor/harbor/src/pkg/artifact"
	projectModel "github.com/goharbor/harbor/src/pkg/project/models"
	repomodel "github.com/goharbor/harbor/src/pkg/repository/model"
)

type abstractorFunc func(context.Context, *artifact.Artifact) error

func (f abstractorFunc) AbstractMetadata(ctx context.Context, art *artifact.Artifact) error {
	return f(ctx, art)
}

// copying an index with a signed child attaches the signature to the child only
func (c *controllerTestSuite) TestCopyIndexWithSignedChild() {
	commondao.PrepareTestForPostgresSQL()
	ctx := orm.Context()
	const (
		srcRepo = "library/copy-src"
		dstRepo = "library/copy-dst"
	)
	artMgr := artifact.NewManager()
	accMgr := accessory.NewManager()
	c.ctl.artMgr = artMgr
	c.ctl.accessoryMgr = accMgr
	defer func() {
		o, err := orm.FromContext(ctx)
		c.Require().NoError(err)
		for _, sql := range []string{
			`DELETE FROM artifact_accessory WHERE subject_artifact_repo IN (?, ?)`,
			`DELETE FROM artifact_reference WHERE parent_id IN (SELECT id FROM artifact WHERE repository_name IN (?, ?))`,
			`DELETE FROM artifact WHERE repository_name IN (?, ?)`,
		} {
			_, err = o.Raw(sql, srcRepo, dstRepo).Exec()
			c.Require().NoError(err)
		}
	}()

	c.tagCtl.On("List").Return([]*tag.Tag{}, nil)
	c.repoMgr.On("GetByName", mock.Anything, dstRepo).Return(&repomodel.RepoRecord{RepositoryID: 9002, ProjectID: 1, Name: dstRepo}, nil)
	c.repoMgr.On("Touch", mock.Anything, int64(9002)).Return(nil)
	c.proCtl.On("GetByName", mock.Anything, "library").Return(&projectModel.Project{ProjectID: 1, Name: "library"}, nil)
	c.regCli.On("Copy", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	newArtifact := func(name, mediaType string) *artifact.Artifact {
		return &artifact.Artifact{
			ProjectID:         1,
			RepositoryID:      9001,
			RepositoryName:    srcRepo,
			Digest:            digest.FromString(name + time.Now().String()).String(),
			Type:              "IMAGE",
			MediaType:         mediaType,
			ManifestMediaType: mediaType,
			PushTime:          time.Now(),
		}
	}
	child := newArtifact("child", v1.MediaTypeImageManifest)
	sig := newArtifact("signature", v1.MediaTypeImageManifest)
	index := newArtifact("index", v1.MediaTypeImageIndex)
	var err error
	child.ID, err = artMgr.Create(ctx, child)
	c.Require().NoError(err)
	sig.ID, err = artMgr.Create(ctx, sig)
	c.Require().NoError(err)
	index.References = []*artifact.Reference{{ChildID: child.ID, ChildDigest: child.Digest}}
	index.ID, err = artMgr.Create(ctx, index)
	c.Require().NoError(err)
	_, err = accMgr.Create(ctx, accessorymodel.AccessoryData{
		ArtifactID:        sig.ID,
		SubArtifactID:     child.ID,
		SubArtifactRepo:   srcRepo,
		SubArtifactDigest: child.Digest,
		Digest:            sig.Digest,
		Type:              accessorymodel.TypeCosignSignature,
	})
	c.Require().NoError(err)

	c.ctl.abstractor = abstractorFunc(func(ctx context.Context, art *artifact.Artifact) error {
		art.MediaType = v1.MediaTypeImageManifest
		art.ManifestMediaType = v1.MediaTypeImageManifest
		if art.Digest == index.Digest {
			art.MediaType = v1.MediaTypeImageIndex
			art.ManifestMediaType = v1.MediaTypeImageIndex
			dstChild, err := artMgr.GetByDigest(ctx, art.RepositoryName, child.Digest)
			if err != nil {
				return err
			}
			art.References = []*artifact.Reference{{ChildID: dstChild.ID, ChildDigest: child.Digest}}
		}
		return nil
	})
	err = orm.WithTransaction(func(ctx context.Context) error {
		_, err := c.ctl.Copy(ctx, srcRepo, index.Digest, dstRepo)
		return err
	})(ctx)
	c.Require().NoError(err)

	accs, err := accMgr.List(ctx, q.New(q.KeyWords{"SubjectArtifactRepo": dstRepo}))
	c.Require().NoError(err)
	c.Require().Len(accs, 1)
	c.Equal(child.Digest, accs[0].GetData().SubArtifactDigest)
}
