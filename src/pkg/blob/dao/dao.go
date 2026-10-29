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
	"fmt"
	"strings"
	"time"

	beegoorm "github.com/beego/beego/v2/client/orm"
	"github.com/docker/distribution/manifest/schema2"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/log"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/q"
	"github.com/goharbor/harbor/src/pkg/blob/models"
)

// DAO the dao for Blob, ArtifactAndBlob and ProjectBlob
type DAO interface {
	// CreateArtifactAndBlob create ArtifactAndBlob and ignore conflict on artifact digest and blob digest
	CreateArtifactAndBlob(ctx context.Context, artifactDigest, blobDigest string) (int64, error)

	// GetArtifactAndBlob get ArtifactAndBlob by artifact digest and blob digest
	GetArtifactAndBlob(ctx context.Context, artifactDigest, blobDigest string) (*models.ArtifactAndBlob, error)

	// DeleteArtifactAndBlobByArtifact delete ArtifactAndBlob by artifact digest
	DeleteArtifactAndBlobByArtifact(ctx context.Context, artifactDigest string) error

	// GetAssociatedBlobDigestsForArtifact returns blob digests which associated with the artifact
	GetAssociatedBlobDigestsForArtifact(ctx context.Context, artifact string) ([]string, error)

	// CreateBlob create blob and ignore conflict on digest
	CreateBlob(ctx context.Context, blob *models.Blob) (int64, error)

	// GetBlobByDigest returns blob by digest
	GetBlobByDigest(ctx context.Context, digest string) (*models.Blob, error)

	// UpdateBlob update blob
	UpdateBlob(ctx context.Context, blob *models.Blob) error

	// UpdateBlob update blob status
	UpdateBlobStatus(ctx context.Context, blob *models.Blob) (int64, error)

	// ListBlobs list blobs by query
	ListBlobs(ctx context.Context, query *q.Query) ([]*models.Blob, error)

	// FindBlobsShouldUnassociatedWithProject filter the blobs which should not be associated with the project
	FindBlobsShouldUnassociatedWithProject(ctx context.Context, projectID int64, blobs []*models.Blob) ([]*models.Blob, error)

	// SumBlobsSizeByProject returns sum size of blobs by project, skip foreign blobs when `excludeForeignLayer` is true
	SumBlobsSizeByProject(ctx context.Context, projectID int64, excludeForeignLayer bool) (int64, error)

	// SumBlobsSize returns sum size of all blobs skip foreign blobs when `excludeForeignLayer` is true
	SumBlobsSize(ctx context.Context, excludeForeignLayer bool) (int64, error)

	// CreateProjectBlob create ProjectBlob and ignore conflict on project id and blob id
	CreateProjectBlob(ctx context.Context, projectID, blobID int64) (int64, error)

	// AssociateBlobsWithProject associates the blobs with the project and ignores conflicts,
	// a not found error is returned when any of the blobs does not exist
	AssociateBlobsWithProject(ctx context.Context, projectID int64, blobDigests []string) error

	// DeleteProjectBlob delete project blob
	DeleteProjectBlob(ctx context.Context, projectID int64, blobIDs ...int64) error

	// ExistProjectBlob returns true when ProjectBlob exist
	ExistProjectBlob(ctx context.Context, projectID int64, blobDigest string) (bool, error)

	// DeleteBlob delete blob
	DeleteBlob(ctx context.Context, id int64) (err error)

	// GetBlobsNotRefedByProjectBlob get the blobs that are not referenced by the table project_blob and also not in the reserve window(in hours)
	GetBlobsNotRefedByProjectBlob(ctx context.Context, timeWindowHours int64) ([]*models.Blob, error)

	// GetBlobsByArtDigest get the blobs that are referenced by artifact
	GetBlobsByArtDigest(ctx context.Context, digest string) ([]*models.Blob, error)
}

// New returns an instance of the default DAO
func New() DAO {
	return &dao{}
}

type dao struct{}

func (d *dao) CreateArtifactAndBlob(ctx context.Context, artifactDigest, blobDigest string) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}

	// see CreateProjectBlob for why DO NOTHING is used
	id, err := insertOrGetID(o,
		`INSERT INTO artifact_blob (digest_af, digest_blob, creation_time) VALUES (?, ?, ?)
ON CONFLICT (digest_af, digest_blob) DO NOTHING RETURNING id`,
		`SELECT id FROM artifact_blob WHERE digest_af = ? AND digest_blob = ?`,
		artifactDigest, blobDigest)
	if err != nil {
		return 0, orm.WrapNotFoundError(err, "not found by artifact digest %s and blob digest %s", artifactDigest, blobDigest)
	}

	return id, nil
}

// insertOrGetID runs insert, which returns no row on conflict, then query for the existing id.
// Both are retried as the existing row can be deleted by others in between.
func insertOrGetID(o beegoorm.QueryExecutor, insert, query string, args ...any) (int64, error) {
	var (
		id  int64
		err error
	)
	for range 3 {
		err = o.Raw(insert, append(args, time.Now())...).QueryRow(&id)
		if !errors.Is(err, orm.ErrNoRows) {
			return id, err
		}
		err = o.Raw(query, args...).QueryRow(&id)
		if !errors.Is(err, orm.ErrNoRows) {
			return id, err
		}
	}
	return 0, err
}

func (d *dao) GetArtifactAndBlob(ctx context.Context, artifactDigest, blobDigest string) (*models.ArtifactAndBlob, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	md := &models.ArtifactAndBlob{
		DigestAF:   artifactDigest,
		DigestBlob: blobDigest,
	}

	if err := o.Read(md, "digest_af", "digest_blob"); err != nil {
		return nil, orm.WrapNotFoundError(err, "not found by artifact digest %s and blob digest %s", artifactDigest, blobDigest)
	}

	return md, nil
}

func (d *dao) DeleteArtifactAndBlobByArtifact(ctx context.Context, artifactDigest string) error {
	qs, err := orm.QuerySetter(ctx, &models.ArtifactAndBlob{}, q.New(q.KeyWords{"digest_af": artifactDigest}))
	if err != nil {
		return err
	}

	_, err = qs.Delete()
	return err
}

func (d *dao) GetAssociatedBlobDigestsForArtifact(ctx context.Context, artifact string) ([]string, error) {
	qs, err := orm.QuerySetter(ctx, &models.ArtifactAndBlob{}, q.New(q.KeyWords{"digest_af": artifact}))
	if err != nil {
		return nil, err
	}

	mds := []*models.ArtifactAndBlob{}
	if _, err = qs.All(&mds); err != nil {
		return nil, err
	}

	var blobDigests []string
	for _, md := range mds {
		blobDigests = append(blobDigests, md.DigestBlob)
	}

	return blobDigests, nil
}

func (d *dao) CreateBlob(ctx context.Context, blob *models.Blob) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}

	blob.CreationTime = time.Now()
	// the default status is none
	blob.Status = models.StatusNone

	return o.InsertOrUpdate(blob, "digest")
}

func (d *dao) GetBlobByDigest(ctx context.Context, digest string) (*models.Blob, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	blob := &models.Blob{Digest: digest}
	if err = o.Read(blob, "digest"); err != nil {
		return nil, orm.WrapNotFoundError(err, "blob %s not found", digest)
	}

	return blob, nil
}

func (d *dao) UpdateBlobStatus(ctx context.Context, blob *models.Blob) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return -1, err
	}

	var sql string
	if blob.Status == models.StatusNone {
		sql = `UPDATE blob SET version = version + 1, update_time = ?, status = ? where id = ? AND version >= ? AND status IN (%s) RETURNING version as new_version`
	} else {
		sql = `UPDATE blob SET version = version + 1, update_time = ?, status = ? where id = ? AND version = ? AND status IN (%s) RETURNING version as new_version`
	}

	var newVersion int64
	params := []any{time.Now(), blob.Status, blob.ID, blob.Version}
	stats := models.StatusMap[blob.Status]
	for _, stat := range stats {
		params = append(params, stat)
	}
	if err := o.Raw(fmt.Sprintf(sql, orm.ParamPlaceholderForIn(len(models.StatusMap[blob.Status]))), params...).QueryRow(&newVersion); err != nil {
		if e := orm.AsNotFoundError(err, "no blob is updated"); e != nil {
			log.Warningf("no blob is updated according to query condition, id: %d, status_in, %v, err: %v", blob.ID, models.StatusMap[blob.Status], e)
			return 0, nil
		}
		return -1, err
	}

	blob.Version = newVersion
	return 1, nil
}

// UpdateBlob cannot handle the status change and version increase, for handling blob status change, please call
// for the UpdateBlobStatus.
func (d *dao) UpdateBlob(ctx context.Context, blob *models.Blob) error {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	blob.UpdateTime = time.Now()
	_, err = o.Update(blob, "size", "content_type", "update_time")
	return err
}

func (d *dao) ListBlobs(ctx context.Context, query *q.Query) ([]*models.Blob, error) {
	qs, err := orm.QuerySetter(ctx, &models.Blob{}, query)
	if err != nil {
		return nil, err
	}
	blobs := []*models.Blob{}
	if _, err = qs.All(&blobs); err != nil {
		return nil, err
	}
	return blobs, nil
}

func (d *dao) FindBlobsShouldUnassociatedWithProject(ctx context.Context, projectID int64, blobs []*models.Blob) ([]*models.Blob, error) {
	if len(blobs) == 0 {
		return nil, nil
	}

	o, err := orm.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Build a VALUES list for all blob digests and use EXISTS to check
	// association. EXISTS short-circuits on the first match, and targeting
	// one digest at a time gives the planner high selectivity to use the index.
	valueParts := make([]string, len(blobs))
	params := make([]interface{}, len(blobs)+1)
	for i, blob := range blobs {
		valueParts[i] = "(?)"
		params[i] = blob.Digest
	}
	params[len(blobs)] = projectID

	sql := `SELECT v.digest_blob FROM (VALUES ` + strings.Join(valueParts, ", ") + `) AS v(digest_blob)
WHERE EXISTS (
  SELECT 1 FROM artifact a
  JOIN artifact_blob b ON a.digest = b.digest_af
  WHERE a.project_id = ? AND b.digest_blob = v.digest_blob
)`

	var digests []string
	_, err = o.Raw(sql, params...).QueryRows(&digests)
	if err != nil {
		return nil, err
	}

	shouldAssociated := map[string]bool{}
	for _, digest := range digests {
		shouldAssociated[digest] = true
	}

	var results []*models.Blob
	for _, blob := range blobs {
		if !shouldAssociated[blob.Digest] {
			results = append(results, blob)
		}
	}

	return results, nil
}

func (d *dao) SumBlobsSizeByProject(ctx context.Context, projectID int64, excludeForeignLayer bool) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}

	params := []any{projectID}
	sql := `SELECT SUM(size) FROM blob JOIN project_blob ON blob.id = project_blob.blob_id AND project_id = ?`
	if excludeForeignLayer {
		foreignLayerTypes := []any{
			schema2.MediaTypeForeignLayer,
		}

		sql = fmt.Sprintf(`%s AND content_type NOT IN (%s)`, sql, orm.ParamPlaceholderForIn(len(foreignLayerTypes)))
		params = append(params, foreignLayerTypes...)
	}

	var totalSize int64
	if err := o.Raw(sql, params...).QueryRow(&totalSize); err != nil {
		return 0, err
	}

	return totalSize, nil
}

// SumBlobsSize returns sum size of all blobs skip foreign blobs when `excludeForeignLayer` is true
func (d *dao) SumBlobsSize(ctx context.Context, excludeForeignLayer bool) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}

	params := []any{}
	sql := `SELECT SUM(size) FROM blob`
	if excludeForeignLayer {
		foreignLayerTypes := []any{
			schema2.MediaTypeForeignLayer,
		}
		sql = fmt.Sprintf(`%s Where content_type NOT IN (%s)`, sql, orm.ParamPlaceholderForIn(len(foreignLayerTypes)))
		params = append(params, foreignLayerTypes...)
	}

	var totalSize int64
	if err := o.Raw(sql, params...).QueryRow(&totalSize); err != nil {
		return 0, err
	}

	return totalSize, nil
}

func (d *dao) CreateProjectBlob(ctx context.Context, projectID, blobID int64) (int64, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}

	// DO NOTHING instead of DO UPDATE, the latter row-locks the existing association until
	// the transaction ends, which deadlocks concurrent pushes sharing blobs
	id, err := insertOrGetID(o,
		`INSERT INTO project_blob (project_id, blob_id, creation_time) VALUES (?, ?, ?)
ON CONFLICT (project_id, blob_id) DO NOTHING RETURNING id`,
		`SELECT id FROM project_blob WHERE project_id = ? AND blob_id = ?`,
		projectID, blobID)
	if err != nil {
		return 0, orm.WrapNotFoundError(err, "project blob not found by project id %d and blob id %d", projectID, blobID)
	}

	return id, nil
}

func (d *dao) AssociateBlobsWithProject(ctx context.Context, projectID int64, blobDigests []string) error {
	digests := make([]any, 0, len(blobDigests))
	seen := make(map[string]struct{}, len(blobDigests))
	for _, digest := range blobDigests {
		if _, ok := seen[digest]; !ok {
			seen[digest] = struct{}{}
			digests = append(digests, digest)
		}
	}
	if len(digests) == 0 {
		return nil
	}

	o, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}

	// insert the rows sorted by blob id: inserting a row that another in-progress transaction
	// is inserting waits for it, so concurrent transactions must use the same order to avoid deadlocks
	var blobs []*models.Blob
	sql := fmt.Sprintf(`SELECT id, digest FROM blob WHERE digest IN (%s) ORDER BY id`, orm.ParamPlaceholderForIn(len(digests)))
	if _, err := o.Raw(sql, digests...).QueryRows(&blobs); err != nil {
		return err
	}

	if len(blobs) != len(digests) {
		found := make(map[string]struct{}, len(blobs))
		for _, blob := range blobs {
			found[blob.Digest] = struct{}{}
		}
		for _, digest := range digests {
			if _, ok := found[digest.(string)]; !ok {
				return errors.NotFoundError(nil).WithMessagef("blob %s not found", digest)
			}
		}
	}

	// see CreateProjectBlob for why DO NOTHING is used
	now := time.Now()
	values := make([]string, 0, len(blobs))
	params := make([]any, 0, len(blobs)*3)
	for _, blob := range blobs {
		values = append(values, "(?, ?, ?)")
		params = append(params, projectID, blob.ID, now)
	}
	sql = fmt.Sprintf(`INSERT INTO project_blob (project_id, blob_id, creation_time) VALUES %s
ON CONFLICT (project_id, blob_id) DO NOTHING`, strings.Join(values, ", "))
	_, err = o.Raw(sql, params...).Exec()
	return err
}

func (d *dao) ExistProjectBlob(ctx context.Context, projectID int64, blobDigest string) (bool, error) {
	o, err := orm.FromContext(ctx)
	if err != nil {
		return false, err
	}

	sql := `SELECT COUNT(*) FROM project_blob JOIN blob ON project_blob.blob_id = blob.id AND project_id = ? AND digest = ?`

	var count int64
	if err := o.Raw(sql, projectID, blobDigest).QueryRow(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}

func (d *dao) DeleteProjectBlob(ctx context.Context, projectID int64, blobIDs ...int64) error {
	if len(blobIDs) == 0 {
		return nil
	}
	ol := &q.OrList{}
	for _, blobID := range blobIDs {
		ol.Values = append(ol.Values, blobID)
	}
	kw := q.KeyWords{"blob_id": ol, "project_id": projectID}
	qs, err := orm.QuerySetter(ctx, &models.ProjectBlob{}, q.New(kw))
	if err != nil {
		return err
	}

	_, err = qs.Delete()
	return err
}

func (d *dao) DeleteBlob(ctx context.Context, id int64) error {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	n, err := ormer.Delete(&models.Blob{
		ID: id,
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.NotFoundError(nil).WithMessagef("blob %d not found", id)
	}
	return nil
}

func (d *dao) GetBlobsNotRefedByProjectBlob(ctx context.Context, timeWindowHours int64) ([]*models.Blob, error) {
	var noneRefed []*models.Blob
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return noneRefed, err
	}

	sql := fmt.Sprintf(`SELECT b.id, b.digest, b.content_type, b.status, b.version, b.size FROM blob AS b LEFT JOIN project_blob pb ON b.id = pb.blob_id WHERE pb.id IS NULL AND b.update_time <= now() - interval '%d hours';`, timeWindowHours)
	_, err = ormer.Raw(sql).QueryRows(&noneRefed)
	if err != nil {
		return noneRefed, err
	}

	return noneRefed, nil
}

func (d *dao) GetBlobsByArtDigest(ctx context.Context, digest string) ([]*models.Blob, error) {
	var blobs []*models.Blob
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return blobs, err
	}

	sql := `SELECT b.id, b.digest, b.content_type, b.status, b.version, b.size FROM artifact_blob AS ab LEFT JOIN blob b ON ab.digest_blob = b.digest WHERE ab.digest_af = ?`
	_, err = ormer.Raw(sql, digest).QueryRows(&blobs)
	if err != nil {
		return blobs, err
	}

	return blobs, nil
}
