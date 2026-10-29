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

package session

import (
	"context"
	"testing"
	"time"

	"github.com/beego/beego/v2/server/web/session"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"

	"github.com/goharbor/harbor/src/lib"
	"github.com/goharbor/harbor/src/lib/cache/redis"
	"github.com/goharbor/harbor/src/lib/config"
	"github.com/goharbor/harbor/src/lib/metric"
	_ "github.com/goharbor/harbor/src/pkg/config/db"
	_ "github.com/goharbor/harbor/src/pkg/config/inmemory"
)

const redisURL = "redis://127.0.0.1:6379/0"

func sessionCreated() float64 {
	m := &dto.Metric{}
	_ = metric.SessionCreatedTotal.Write(m)
	return m.GetCounter().GetValue()
}

type sessionTestSuite struct {
	suite.Suite

	provider session.Provider
}

func (s *sessionTestSuite) SetupSuite() {
	config.Init()

	var err error
	s.provider, err = session.GetProvider("harbor")
	s.NoError(err, "should get harbor provider")
	s.NotNil(s.provider, "provider should not nil")

	err = s.provider.SessionInit(context.Background(), 3600, redisURL)
	s.NoError(err, "session init should not error")
}

func (s *sessionTestSuite) TestSessionRead() {
	store, err := s.provider.SessionRead(context.Background(), "session-001")
	s.NoError(err, "session read should not error")
	s.NotNil(store)
}

func (s *sessionTestSuite) TestSessionExist() {
	// prepare session
	ctx := context.Background()
	store, err := s.provider.SessionRead(ctx, "session-001")
	s.NoError(err, "session read should not error")
	s.NotNil(store)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(context.Background(), nil)

	defer func() {
		// clean session
		err = s.provider.SessionDestroy(ctx, "session-001")
		s.NoError(err)
	}()

	exist, _ := s.provider.SessionExist(ctx, "session-001")
	s.True(exist, "session-001 should exist")

	exist, _ = s.provider.SessionExist(ctx, "session-002")
	s.False(exist, "session-002 should not exist")
}

func (s *sessionTestSuite) TestSessionRegenerate() {
	// prepare session
	ctx := context.Background()
	store, err := s.provider.SessionRead(ctx, "session-001")
	s.NoError(err, "session read should not error")
	s.NotNil(store)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)

	defer func() {
		// clean session
		err = s.provider.SessionDestroy(ctx, "session-001")
		s.NoError(err)

		err = s.provider.SessionDestroy(ctx, "session-003")
		s.NoError(err)

		err = s.provider.SessionDestroy(ctx, "session-004")
		s.NoError(err)
	}()

	_, err = s.provider.SessionRegenerate(ctx, "session-001", "session-003")
	s.NoError(err, "session regenerate should not error")

	s.True(s.provider.SessionExist(ctx, "session-003"))
	s.False(s.provider.SessionExist(ctx, "session-001"))

	// the old session doesn't exist, the new one isn't persisted until it holds data
	store, err = s.provider.SessionRegenerate(ctx, "session-001", "session-004")
	s.NoError(err, "session regenerate should not error")
	s.False(s.provider.SessionExist(ctx, "session-004"))
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)
	s.True(s.provider.SessionExist(ctx, "session-004"))
}

func (s *sessionTestSuite) TestSessionDestroy() {
	// prepare session
	ctx := context.Background()
	store, err := s.provider.SessionRead(ctx, "session-004")
	s.NoError(err, "session read should not error")
	s.NotNil(store)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)
	isExist, _ := s.provider.SessionExist(ctx, "session-004")
	s.True(isExist, "session-004 should exist")

	err = s.provider.SessionDestroy(ctx, "session-004")
	s.NoError(err, "session destroy should not error")
	isExist, _ = s.provider.SessionExist(ctx, "session-004")
	s.False(isExist, "session-004 should not exist")
}

func (s *sessionTestSuite) TestReleaseNewEmptySession() {
	ctx := context.Background()
	created := sessionCreated()

	store, err := s.provider.SessionRead(ctx, "session-005")
	s.NoError(err)
	store.SessionRelease(ctx, nil)
	store.SessionReleaseIfPresent(ctx, nil)
	exist, _ := s.provider.SessionExist(ctx, "session-005")
	s.False(exist, "empty new session should not be persisted")
	s.Equal(created, sessionCreated())
}

func (s *sessionTestSuite) TestReleaseNewSessionWithData() {
	ctx := context.Background()
	defer s.provider.SessionDestroy(ctx, "session-006")
	created := sessionCreated()

	store, err := s.provider.SessionRead(ctx, "session-006")
	s.NoError(err)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)
	// releasing again must not count the session twice
	store.SessionRelease(ctx, nil)
	s.True(s.provider.SessionExist(ctx, "session-006"))
	s.Equal(created+1, sessionCreated())

	// a persisted session is still saved after its data is removed
	store, err = s.provider.SessionRead(ctx, "session-006")
	s.NoError(err)
	s.NoError(store.Flush(ctx))
	store.SessionRelease(ctx, nil)
	s.True(s.provider.SessionExist(ctx, "session-006"))
	s.Equal(created+1, sessionCreated())
}

func (s *sessionTestSuite) TestReleaseNewSessionSkipRenewal() {
	ctx := lib.WithSkipSessionRenewal(context.Background(), true)
	defer s.provider.SessionDestroy(ctx, "session-007")

	store, err := s.provider.SessionRead(ctx, "session-007")
	s.NoError(err)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)
	// a new session must get an expiration even if the renewal is skipped
	ttl, err := store.(*Store).c.(*redis.Cache).Client.TTL(ctx, "session-007").Result()
	s.NoError(err)
	s.Greater(ttl, time.Duration(0))
}

func (s *sessionTestSuite) TestReleaseExpiredSessionSkipRenewal() {
	ctx := context.Background()
	defer s.provider.SessionDestroy(ctx, "session-008")

	store, err := s.provider.SessionRead(ctx, "session-008")
	s.NoError(err)
	s.NoError(store.Set(ctx, "key", "value"))
	store.SessionRelease(ctx, nil)

	store, err = s.provider.SessionRead(ctx, "session-008")
	s.NoError(err)
	// the session expires while the request is being served
	s.NoError(s.provider.SessionDestroy(ctx, "session-008"))
	store.SessionRelease(lib.WithSkipSessionRenewal(ctx, true), nil)
	exist, _ := s.provider.SessionExist(ctx, "session-008")
	s.False(exist, "expired session should not be recreated")
}

func (s *sessionTestSuite) TestSessionGC() {
	s.provider.SessionGC(context.Background())
}

func (s *sessionTestSuite) TestSessionAll() {
	c := s.provider.SessionAll(context.Background())
	s.Equal(0, c)
}

func TestSession(t *testing.T) {
	suite.Run(t, &sessionTestSuite{})
}
