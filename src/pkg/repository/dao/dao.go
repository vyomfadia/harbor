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
	"time"

	o "github.com/beego/beego/v2/client/orm"

	"github.com/goharbor/harbor/src/lib/errors"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/lib/q"
	"github.com/goharbor/harbor/src/pkg/repository/model"
)

// DAO is the data access object interface for repository
type DAO interface {
	// Count returns the total count of repositories according to the query
	Count(ctx context.Context, query *q.Query) (count int64, err error)
	// List repositories according to the query
	List(ctx context.Context, query *q.Query) (repositories []*model.RepoRecord, err error)
	// Get the repository specified by ID
	Get(ctx context.Context, id int64) (repository *model.RepoRecord, err error)
	// Create the repository
	Create(ctx context.Context, repository *model.RepoRecord) (id int64, err error)
	// Ensure creates the repository if no repository with the same name exists,
	// and returns the ID of the created or existing one
	Ensure(ctx context.Context, repository *model.RepoRecord) (created bool, id int64, err error)
	// Delete the repository specified by ID
	Delete(ctx context.Context, id int64) (err error)
	// Update updates the repository. Only the properties specified by "props" will be updated if it is set
	Update(ctx context.Context, repository *model.RepoRecord, props ...string) (err error)
	// AddPullCount increase pull count for the specified repository
	AddPullCount(ctx context.Context, id int64, count uint64) error
	// Touch bumps the repository's update_time to now
	Touch(ctx context.Context, id int64) error
	// NonEmptyRepos returns the repositories without any artifact or all the artifacts are untagged.
	NonEmptyRepos(ctx context.Context) ([]*model.RepoRecord, error)
}

// New returns an instance of the default DAO
func New() DAO {
	return &dao{}
}

type dao struct{}

func (d *dao) Count(ctx context.Context, query *q.Query) (int64, error) {
	qs, err := orm.QuerySetterForCount(ctx, &model.RepoRecord{}, query)
	if err != nil {
		return 0, err
	}
	return qs.Count()
}
func (d *dao) List(ctx context.Context, query *q.Query) ([]*model.RepoRecord, error) {
	repositories := []*model.RepoRecord{}
	qs, err := orm.QuerySetter(ctx, &model.RepoRecord{}, query)
	if err != nil {
		return nil, err
	}
	if _, err = qs.All(&repositories); err != nil {
		return nil, err
	}
	return repositories, nil
}

func (d *dao) Get(ctx context.Context, id int64) (*model.RepoRecord, error) {
	repository := &model.RepoRecord{
		RepositoryID: id,
	}
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := ormer.Read(repository); err != nil {
		if e := orm.AsNotFoundError(err, "repository %d not found", id); e != nil {
			err = e
		}
		return nil, err
	}
	return repository, nil
}

func (d *dao) Create(ctx context.Context, repository *model.RepoRecord) (int64, error) {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return 0, err
	}
	id, err := ormer.Insert(repository)
	if e := orm.AsConflictError(err, "repository %s already exists", repository.Name); e != nil {
		err = e
	}
	return id, err
}

func (d *dao) Ensure(ctx context.Context, repository *model.RepoRecord) (bool, int64, error) {
	// fill the auto_now(_add) columns as Insert does
	now := time.Now()
	if repository.CreationTime.IsZero() {
		repository.CreationTime = now
	}
	if repository.UpdateTime.IsZero() {
		repository.UpdateTime = now
	}
	id, created, err := orm.InsertOrGetID(ctx,
		`INSERT INTO repository (name, project_id, description, pull_count, star_count, creation_time, update_time)
VALUES (?, ?, ?, ?, ?, ?::timestamptz, ?::timestamptz)
ON CONFLICT (name) DO NOTHING RETURNING repository_id`,
		[]any{repository.Name, repository.ProjectID, repository.Description, repository.PullCount, repository.StarCount,
			orm.TimeArg(repository.CreationTime), orm.TimeArg(repository.UpdateTime)},
		`SELECT repository_id FROM repository WHERE name = ?`,
		repository.Name)
	if err != nil {
		return false, 0, err
	}
	return created, id, nil
}

func (d *dao) Delete(ctx context.Context, id int64) error {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	n, err := ormer.Delete(&model.RepoRecord{
		RepositoryID: id,
	})
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.NotFoundError(nil).WithMessagef("repository %d not found", id)
	}
	return nil
}

func (d *dao) Update(ctx context.Context, repository *model.RepoRecord, props ...string) error {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	n, err := ormer.Update(repository, props...)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.NotFoundError(nil).WithMessagef("repository %d not found", repository.RepositoryID)
	}
	return nil
}

func (d *dao) AddPullCount(ctx context.Context, id int64, count uint64) error {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	num, err := ormer.QueryTable(new(model.RepoRecord)).Filter("RepositoryID", id).Update(
		o.Params{
			"pull_count":  o.ColValue(o.ColAdd, count),
			"update_time": time.Now(),
		})
	if err != nil {
		return err
	}
	if num == 0 {
		return errors.New(nil).WithMessagef("failed to increase repository pull count: %d", id)
	}
	return nil
}

func (d *dao) Touch(ctx context.Context, id int64) error {
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return err
	}
	num, err := ormer.QueryTable(new(model.RepoRecord)).Filter("RepositoryID", id).Update(
		o.Params{"update_time": time.Now()})
	if err != nil {
		return err
	}
	if num == 0 {
		return errors.NotFoundError(nil).WithMessagef("repository %d not found", id)
	}
	return nil
}

func (d *dao) NonEmptyRepos(ctx context.Context) ([]*model.RepoRecord, error) {
	var repos []*model.RepoRecord
	ormer, err := orm.FromContext(ctx)
	if err != nil {
		return nil, err
	}

	sql := `select * from repository where exists (select 1 from tag where tag.repository_id = repository.repository_id)`
	_, err = ormer.Raw(sql).QueryRows(&repos)
	if err != nil {
		return repos, err
	}

	return repos, nil
}
