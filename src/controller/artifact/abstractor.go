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
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/goharbor/harbor/src/controller/artifact/manifest"
	"github.com/goharbor/harbor/src/controller/artifact/processor"
	"github.com/goharbor/harbor/src/pkg/artifact"
	"github.com/goharbor/harbor/src/pkg/registry"
)

// Abstractor abstracts the metadata of artifact
type Abstractor interface {
	// AbstractMetadata abstracts the metadata for the specific artifact type into the artifact model,
	AbstractMetadata(ctx context.Context, artifact *artifact.Artifact) error
}

// NewAbstractor creates a new abstractor
func NewAbstractor() Abstractor {
	return &abstractor{
		regCli: registry.Cli,
	}
}

type abstractor struct {
	regCli registry.Client
}

func (a *abstractor) AbstractMetadata(ctx context.Context, art *artifact.Artifact) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if prepared, ok := ctx.Value(preparedMetadataKey{}).(*preparedMetadata); ok {
		if prepared.repository != art.RepositoryName || prepared.digest != art.Digest {
			return fmt.Errorf("prepared metadata does not match artifact %s", art)
		}
		art.ManifestMediaType = prepared.mediaType
		// Re-read database-derived sizes and child IDs on every attempt. Never carry
		// references or mutable models from a rolled-back transaction into a retry.
		abs, err := manifest.Get(art.ManifestMediaType)
		if err != nil {
			return err
		}
		if err := abs.Abstract(ctx, art, prepared.content); err != nil {
			return err
		}
		art.Icon = prepared.icon
		decoder := json.NewDecoder(bytes.NewReader(prepared.extraAttrs))
		decoder.UseNumber() // preserve integer precision when metadata is persisted again
		return decoder.Decode(&art.ExtraAttrs)
	}
	// read manifest content
	mf, _, err := a.regCli.PullManifest(art.RepositoryName, art.Digest)
	if err != nil {
		return err
	}
	manifestMediaType, content, err := mf.Payload()
	if err != nil {
		return err
	}
	art.ManifestMediaType = manifestMediaType

	abs, err := manifest.Get(art.ManifestMediaType)
	if err != nil {
		return err
	}
	if err = abs.Abstract(ctx, art, content); err != nil {
		return err
	}

	return processor.Get(art.ResolveArtifactType()).AbstractMetadata(ctx, art, content)
}

type preparedMetadataKey struct{}

type preparedMetadata struct {
	repository, digest, mediaType, icon string
	content, extraAttrs                 []byte
}

// PrepareMetadata completes registry I/O before the push transaction begins.
// Only content-derived metadata is retained. Database-derived references and
// sizes are resolved again by AbstractMetadata inside the committing transaction.
func PrepareMetadata(ctx context.Context, repository, digest string) (context.Context, error) {
	return (&abstractor{regCli: registry.Cli}).prepareMetadata(ctx, repository, digest)
}

func (a *abstractor) prepareMetadata(ctx context.Context, repository, digest string) (context.Context, error) {
	mf, _, err := a.regCli.PullManifest(repository, digest)
	if err != nil {
		return ctx, err
	}
	mediaType, content, err := mf.Payload()
	if err != nil {
		return ctx, err
	}
	art := &artifact.Artifact{RepositoryName: repository, Digest: digest, ManifestMediaType: mediaType}
	abs, err := manifest.Get(mediaType)
	if err != nil {
		return ctx, err
	}
	if err := abs.Abstract(ctx, art, content); err != nil {
		return ctx, err
	}
	if err := processor.Get(art.ResolveArtifactType()).AbstractMetadata(ctx, art, content); err != nil {
		return ctx, err
	}
	attrs, err := json.Marshal(art.ExtraAttrs)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, preparedMetadataKey{}, &preparedMetadata{
		repository: repository, digest: digest, mediaType: mediaType, content: content,
		icon: art.Icon, extraAttrs: attrs,
	}), nil
}
