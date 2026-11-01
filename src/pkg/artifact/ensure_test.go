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
	"testing"
	"time"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/lib/q"
	htesting "github.com/goharbor/harbor/src/testing"
	ormtesting "github.com/goharbor/harbor/src/testing/lib/orm"
)

type ensureTestSuite struct {
	htesting.Suite
	mgr Manager
}

func (s *ensureTestSuite) SetupSuite() {
	s.Suite.SetupSuite()
	s.mgr = NewManager()
}

func (s *ensureTestSuite) TestEnsureIndexConcurrently() {
	ctx := s.Context()
	repositoryID := int64(2001)
	newArtifact := func(digest, mediaType string) *Artifact {
		return &Artifact{
			Type:              "IMAGE",
			MediaType:         v1.MediaTypeImageConfig,
			ManifestMediaType: mediaType,
			ProjectID:         1,
			RepositoryID:      repositoryID,
			RepositoryName:    "library/index",
			Digest:            digest,
			PushTime:          time.Now(),
		}
	}

	var children []*Artifact
	for range 2 {
		child := newArtifact(s.DigestString(), v1.MediaTypeImageManifest)
		created, id, err := s.mgr.Ensure(ctx, child)
		s.Require().NoError(err)
		s.Require().True(created)
		child.ID = id
		children = append(children, child)
	}

	indexDigest := s.DigestString()
	result := ormtesting.EnsureConcurrently(ctx, 16, func(ctx context.Context) (bool, int64, error) {
		index := newArtifact(indexDigest, v1.MediaTypeImageIndex)
		for _, child := range children {
			index.References = append(index.References, &Reference{ChildID: child.ID, ChildDigest: child.Digest})
		}
		return s.mgr.Ensure(ctx, index)
	})
	s.Require().Empty(result.Errs)
	s.Equal(1, result.Created)
	s.Require().Len(result.IDs, 16)
	for _, id := range result.IDs {
		s.Equal(result.IDs[0], id)
	}

	total, err := s.mgr.Count(ctx, q.New(q.KeyWords{"RepositoryID": repositoryID, "Digest": indexDigest}))
	s.Require().NoError(err)
	s.Equal(int64(1), total)
	refs, err := s.mgr.ListReferences(ctx, q.New(q.KeyWords{"ParentID": result.IDs[0]}))
	s.Require().NoError(err)
	s.Len(refs, len(children))

	s.Require().NoError(s.mgr.Delete(ctx, result.IDs[0]))
	for _, child := range children {
		s.Require().NoError(s.mgr.Delete(ctx, child.ID))
	}
}

func TestEnsureTestSuite(t *testing.T) {
	suite.Run(t, &ensureTestSuite{})
}
