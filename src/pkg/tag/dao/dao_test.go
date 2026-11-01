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
	"testing"
	"time"

	beegoorm "github.com/beego/beego/v2/client/orm"
	"github.com/stretchr/testify/suite"

	common_dao "github.com/goharbor/harbor/src/common/dao"
	errors "github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/q"
	artdao "github.com/goharbor/harbor/src/pkg/artifact/dao"
	"github.com/goharbor/harbor/src/pkg/tag/model/tag"
	ormtesting "github.com/goharbor/harbor/src/testing/lib/orm"
)

type daoTestSuite struct {
	suite.Suite
	dao        DAO
	artDAO     artdao.DAO
	tagID      int64
	artifactID int64
	ctx        context.Context
}

func (d *daoTestSuite) SetupSuite() {
	d.dao = New()
	common_dao.PrepareTestForPostgresSQL()
	d.ctx = orm.NewContext(nil, beegoorm.NewOrm())
	d.artDAO = artdao.New()
	artifactID, err := d.artDAO.Create(d.ctx, &artdao.Artifact{
		Type:              "IMAGE",
		MediaType:         "application/vnd.oci.image.config.v1+json",
		ManifestMediaType: "application/vnd.oci.image.manifest.v1+json",
		ProjectID:         1,
		RepositoryID:      1000,
		Digest:            "sha256:digest",
	})
	d.Require().Nil(err)
	d.artifactID = artifactID
}

func (d *daoTestSuite) TearDownSuite() {
	err := d.artDAO.Delete(d.ctx, d.artifactID)
	d.Require().Nil(err)
}

func (d *daoTestSuite) SetupTest() {
	tag := &tag.Tag{
		RepositoryID: 1000,
		ArtifactID:   d.artifactID,
		Name:         "latest",
		PushTime:     time.Time{},
		PullTime:     time.Time{},
	}
	id, err := d.dao.Create(d.ctx, tag)
	d.Require().Nil(err)
	d.tagID = id
}

func (d *daoTestSuite) TearDownTest() {
	err := d.dao.Delete(d.ctx, d.tagID)
	d.Require().Nil(err)
}

func (d *daoTestSuite) TestCount() {
	// nil query
	total, err := d.dao.Count(d.ctx, nil)
	d.Require().Nil(err)
	d.True(total > 0)
	// query by repository ID and name
	total, err = d.dao.Count(d.ctx, &q.Query{
		Keywords: map[string]any{
			"repository_id": 1000,
			"name":          "latest",
		},
	})
	d.Require().Nil(err)
	d.Equal(int64(1), total)
}

func (d *daoTestSuite) TestList() {
	// nil query
	tags, err := d.dao.List(d.ctx, nil)
	d.Require().Nil(err)
	found := false
	for _, tag := range tags {
		if tag.ID == d.tagID {
			found = true
			break
		}
	}
	d.True(found)

	// query by repository ID and name
	tags, err = d.dao.List(d.ctx, &q.Query{
		Keywords: map[string]any{
			"repository_id": 1000,
			"name":          "latest",
		},
	})
	d.Require().Nil(err)
	d.Require().Equal(1, len(tags))
	d.Equal(d.tagID, tags[0].ID)
}

func (d *daoTestSuite) TestGet() {
	// get the non-exist tag
	_, err := d.dao.Get(d.ctx, 10000)
	d.Require().NotNil(err)
	d.True(errors.IsErr(err, errors.NotFoundCode))

	// get the exist tag
	tag, err := d.dao.Get(d.ctx, d.tagID)
	d.Require().Nil(err)
	d.Require().NotNil(tag)
	d.Equal(d.tagID, tag.ID)
}

func (d *daoTestSuite) TestCreate() {
	// the happy pass case is covered in Setup

	// conflict
	tg := &tag.Tag{
		RepositoryID: 1000,
		ArtifactID:   d.artifactID,
		Name:         "latest",
		PushTime:     time.Time{},
		PullTime:     time.Time{},
	}
	_, err := d.dao.Create(d.ctx, tg)
	d.Require().NotNil(err)
	d.True(errors.IsErr(err, errors.ConflictCode))

	// violating foreign key constraint: the artifact that the tag tries to attach doesn't exist
	tg = &tag.Tag{
		RepositoryID: 1000,
		ArtifactID:   1000,
		Name:         "latest2",
		PushTime:     time.Time{},
		PullTime:     time.Time{},
	}
	_, err = d.dao.Create(d.ctx, tg)
	d.Require().NotNil(err)
	d.True(errors.IsErr(err, errors.ViolateForeignKeyConstraintCode))
}

func (d *daoTestSuite) TestEnsure() {
	pushTime := time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC)
	for _, tg := range []tag.Tag{
		{RepositoryID: 1000, ArtifactID: d.artifactID, PushTime: pushTime, PullTime: pushTime.Add(time.Hour)},
		{RepositoryID: 1000, ArtifactID: d.artifactID},
	} {
		created := tg
		created.Name = "created"
		createdID, err := d.dao.Create(d.ctx, &created)
		d.Require().NoError(err)

		ensured := tg
		ensured.Name = "ensured"
		ok, ensuredID, err := d.dao.Ensure(d.ctx, &ensured)
		d.Require().NoError(err)
		d.True(ok)

		// the same row as Create writes
		createdRow, err := ormtesting.RowJSON(d.ctx, "tag", "id", createdID, "id", "name")
		d.Require().NoError(err)
		ensuredRow, err := ormtesting.RowJSON(d.ctx, "tag", "id", ensuredID, "id", "name")
		d.Require().NoError(err)
		d.Equal(createdRow, ensuredRow)

		d.Require().NoError(d.dao.Delete(d.ctx, createdID))
		d.Require().NoError(d.dao.Delete(d.ctx, ensuredID))
	}

	// exists, even attached to another artifact
	ok, id, err := d.dao.Ensure(d.ctx, &tag.Tag{RepositoryID: 1000, ArtifactID: d.artifactID + 1, Name: "latest"})
	d.Require().NoError(err)
	d.False(ok)
	d.Equal(d.tagID, id)
	tg, err := d.dao.Get(d.ctx, id)
	d.Require().NoError(err)
	d.Equal(d.artifactID, tg.ArtifactID)

	// violating foreign key constraint: the artifact that the tag tries to attach doesn't exist
	_, _, err = d.dao.Ensure(d.ctx, &tag.Tag{RepositoryID: 1000, ArtifactID: 1000, Name: "latest2"})
	d.True(errors.IsErr(err, errors.ViolateForeignKeyConstraintCode))
}

func (d *daoTestSuite) TestEnsureConcurrently() {
	tg := &tag.Tag{RepositoryID: 1000, ArtifactID: d.artifactID, Name: "concurrent", PushTime: time.Now()}
	result := ormtesting.EnsureConcurrently(d.ctx, 16, func(ctx context.Context) (bool, int64, error) {
		return d.dao.Ensure(ctx, tg)
	})
	d.Require().Empty(result.Errs)
	d.Equal(1, result.Created)
	d.Require().Len(result.IDs, 16)
	for _, id := range result.IDs {
		d.Equal(result.IDs[0], id)
	}
	total, err := d.dao.Count(d.ctx, q.New(q.KeyWords{"RepositoryID": tg.RepositoryID, "Name": tg.Name}))
	d.Require().NoError(err)
	d.Equal(int64(1), total)
	d.Require().NoError(d.dao.Delete(d.ctx, result.IDs[0]))
}

func (d *daoTestSuite) TestDelete() {
	// happy pass is covered in TearDown

	// not exist
	err := d.dao.Delete(d.ctx, 10000)
	d.Require().NotNil(err)
	var e *errors.Error
	d.Require().True(errors.As(err, &e))
	d.Equal(errors.NotFoundCode, e.Code)
}

func (d *daoTestSuite) TestUpdate() {
	// pass
	artifactID, err := d.artDAO.Create(d.ctx, &artdao.Artifact{
		Type:              "IMAGE",
		MediaType:         "application/vnd.oci.image.config.v1+json",
		ManifestMediaType: "application/vnd.oci.image.manifest.v1+json",
		ProjectID:         1,
		RepositoryID:      1000,
		Digest:            "sha256:digest2",
	})
	d.Require().Nil(err)
	defer func() {
		err := d.artDAO.Delete(d.ctx, artifactID)
		d.Require().Nil(err)
	}()

	err = d.dao.Update(d.ctx, &tag.Tag{
		ID:         d.tagID,
		ArtifactID: artifactID,
	}, "ArtifactID")
	d.Require().Nil(err)

	tg, err := d.dao.Get(d.ctx, d.tagID)
	d.Require().Nil(err)
	d.Require().NotNil(tg)
	d.Equal(artifactID, tg.ArtifactID)

	err = d.dao.Update(d.ctx, &tag.Tag{
		ID:         d.tagID,
		ArtifactID: d.artifactID,
	}, "ArtifactID")
	d.Require().Nil(err)

	// violating foreign key constraint: the artifact that the tag tries to attach doesn't exist
	err = d.dao.Update(d.ctx, &tag.Tag{
		ID:         d.tagID,
		ArtifactID: 2,
	}, "ArtifactID")
	d.Require().NotNil(err)
	d.True(errors.IsErr(err, errors.ViolateForeignKeyConstraintCode))

	// not exist
	err = d.dao.Update(d.ctx, &tag.Tag{
		ID: 10000,
	})
	d.Require().NotNil(err)
	var e *errors.Error
	d.Require().True(errors.As(err, &e))
	d.Equal(errors.NotFoundCode, e.Code)
}

func (d *daoTestSuite) TestDeleteOfArtifact() {
	artifactID, err := d.artDAO.Create(d.ctx, &artdao.Artifact{
		Type:              "IMAGE",
		MediaType:         "application/vnd.oci.image.config.v1+json",
		ManifestMediaType: "application/vnd.oci.image.manifest.v1+json",
		ProjectID:         1,
		RepositoryID:      1000,
		Digest:            "sha256:digest02",
	})
	d.Require().Nil(err)
	defer d.artDAO.Delete(d.ctx, artifactID)

	tag1 := &tag.Tag{
		RepositoryID: 1000,
		ArtifactID:   artifactID,
		Name:         "tag1",
	}
	_, err = d.dao.Create(d.ctx, tag1)
	d.Require().Nil(err)
	tag2 := &tag.Tag{
		RepositoryID: 1000,
		ArtifactID:   artifactID,
		Name:         "tag2",
	}
	_, err = d.dao.Create(d.ctx, tag2)
	d.Require().Nil(err)

	tags, err := d.dao.List(d.ctx, &q.Query{
		Keywords: map[string]any{
			"ArtifactID": artifactID,
		},
	})
	d.Require().Nil(err)
	d.Require().Len(tags, 2)

	err = d.dao.DeleteOfArtifact(d.ctx, artifactID)
	d.Require().Nil(err)

	tags, err = d.dao.List(d.ctx, &q.Query{
		Keywords: map[string]any{
			"ArtifactID": artifactID,
		},
	})
	d.Require().Nil(err)
	d.Require().Len(tags, 0)
}

func TestDaoTestSuite(t *testing.T) {
	suite.Run(t, &daoTestSuite{})
}
