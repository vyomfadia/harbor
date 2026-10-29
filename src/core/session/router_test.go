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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beego/beego/v2/server/web"
	beegosession "github.com/beego/beego/v2/server/web/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/lib/cache/redis"
	"github.com/goharbor/harbor/src/lib/config"
)

const sidPrefix = "harbor-session-test-"

type testController struct {
	web.Controller
}

func (c *testController) Prepare() {
	c.EnableRender = false
}

func (c *testController) Get() {
	if u, ok := c.GetSession("user").(string); ok {
		c.Ctx.WriteString(u)
	}
}

func (c *testController) Login() {
	if err := c.SessionRegenerateID(); err != nil {
		c.CustomAbort(http.StatusInternalServerError, err.Error())
	}
	if err := c.SetSession("user", "admin"); err != nil {
		c.CustomAbort(http.StatusInternalServerError, err.Error())
	}
}

func TestRequestsWithoutSessionCookie(t *testing.T) {
	config.Init()

	web.BConfig.WebConfig.Session.SessionOn = true
	web.BConfig.WebConfig.Session.SessionName = config.SessionCookieName
	var err error
	web.GlobalSessions, err = beegosession.NewManager(HarborProviderName, &beegosession.ManagerConfig{
		CookieName:      config.SessionCookieName,
		Gclifetime:      3600,
		ProviderConfig:  redisURL,
		EnableSetCookie: true,
		SessionIDPrefix: sidPrefix,
	})
	require.NoError(t, err)

	rdb := harborpder.c.(*redis.Cache).Client
	ctx := context.Background()
	countKeys := func() int {
		keys, err := rdb.Keys(ctx, sidPrefix+"*").Result()
		require.NoError(t, err)
		return len(keys)
	}
	require.Equal(t, 0, countKeys())

	router := web.NewControllerRegister()
	router.Add("/v2/", &testController{}, web.WithRouterMethods(&testController{}, "get:Get"))
	router.Add("/c/login", &testController{}, web.WithRouterMethods(&testController{}, "post:Login"))

	serve := func(method, path string, setup func(r *http.Request)) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if setup != nil {
			setup(req)
		}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		return rec
	}

	created := sessionCreated()
	cases := map[string]func(r *http.Request){
		"anonymous":  nil,
		"basic auth": func(r *http.Request) { r.SetBasicAuth("admin", "Harbor12345") },
		"robot":      func(r *http.Request) { r.SetBasicAuth("robot$test", "secret") },
		"bearer":     func(r *http.Request) { r.Header.Set("Authorization", "Bearer token") },
		"stale cookie": func(r *http.Request) {
			r.AddCookie(&http.Cookie{Name: config.SessionCookieName, Value: "not-exist"})
		},
	}
	for name, setup := range cases {
		for i := 0; i < 50; i++ {
			serve(http.MethodGet, "/v2/", setup)
		}
		assert.Equal(t, 0, countKeys(), "no session should be created for %s requests", name)
	}
	assert.Equal(t, created, sessionCreated())

	rec := serve(http.MethodPost, "/c/login", nil)
	assert.Equal(t, 1, countKeys())
	assert.Equal(t, created+1, sessionCreated())
	var cookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == config.SessionCookieName {
			cookie = c
		}
	}
	require.NotNil(t, cookie)
	exist, _ := harborpder.SessionExist(ctx, cookie.Value)
	assert.True(t, exist)

	// the session is used by the requests carrying the cookie and no new session is created
	for i := 0; i < 10; i++ {
		rec = serve(http.MethodGet, "/v2/", func(r *http.Request) { r.AddCookie(cookie) })
		assert.Equal(t, "admin", rec.Body.String())
	}
	assert.Equal(t, 1, countKeys())
	assert.Equal(t, created+1, sessionCreated())

	require.NoError(t, harborpder.SessionDestroy(ctx, cookie.Value))
}
