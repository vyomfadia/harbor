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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beego/beego/v2/server/web"
	beegosession "github.com/beego/beego/v2/server/web/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/lib/cache/redis"
	"github.com/goharbor/harbor/src/lib/config"
	sessionmw "github.com/goharbor/harbor/src/server/middleware/session"
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

func (c *testController) OIDCLogin() {
	if err := c.SetSession("state", "random"); err != nil {
		c.CustomAbort(http.StatusInternalServerError, err.Error())
	}
	c.Ctx.WriteString("redirect")
}

func (c *testController) State() {
	if s, ok := c.GetSession("state").(string); ok {
		c.Ctx.WriteString(s)
	}
}

func (c *testController) Logout() {
	if err := c.DestroySession(); err != nil {
		c.CustomAbort(http.StatusInternalServerError, err.Error())
	}
}

func setupRouter(t *testing.T) (*web.ControllerRegister, func() int) {
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
	t.Cleanup(func() {
		keys, _ := rdb.Keys(ctx, sidPrefix+"*").Result()
		for _, k := range keys {
			rdb.Del(ctx, k)
		}
	})

	router := web.NewControllerRegister()
	router.Add("/v2/", &testController{}, web.WithRouterMethods(&testController{}, "get:Get"))
	router.Add("/c/login", &testController{}, web.WithRouterMethods(&testController{}, "post:Login"))
	router.Add("/c/oidc/login", &testController{}, web.WithRouterMethods(&testController{}, "get:OIDCLogin"))
	router.Add("/c/oidc/callback", &testController{}, web.WithRouterMethods(&testController{}, "get:State"))
	router.Add("/c/log_out", &testController{}, web.WithRouterMethods(&testController{}, "get:Logout"))
	router.InsertFilterChain("*", CookieFilterChain)
	router.Init()
	return router, countKeys
}

func TestRequestsWithoutSessionCookie(t *testing.T) {
	router, countKeys := setupRouter(t)
	ctx := context.Background()

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
}

// client keeps the cookies like a browser does, the responses are applied explicitly
// so that the order in which they arrive can be controlled
type client struct {
	t       *testing.T
	handler http.Handler
	cookies map[string]*http.Cookie
}

func newClient(t *testing.T, router http.Handler) *client {
	return &client{t: t, handler: sessionmw.Middleware()(router), cookies: map[string]*http.Cookie{}}
}

func (c *client) send(method, path string) *http.Response {
	req := httptest.NewRequest(method, path, nil)
	for _, ck := range c.cookies {
		req.AddCookie(ck)
	}
	rec := httptest.NewRecorder()
	c.handler.ServeHTTP(rec, req)
	require.Equal(c.t, http.StatusOK, rec.Code)
	return rec.Result()
}

func (c *client) receive(resp *http.Response) {
	for _, ck := range resp.Cookies() {
		if ck.MaxAge < 0 {
			delete(c.cookies, ck.Name)
		} else {
			c.cookies[ck.Name] = ck
		}
	}
}

func (c *client) do(method, path string) string {
	resp := c.send(method, path)
	c.receive(resp)
	body, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	return string(body)
}

func sessionCookies(resp *http.Response) []*http.Cookie {
	var cookies []*http.Cookie
	for _, ck := range resp.Cookies() {
		if ck.Name == config.SessionCookieName {
			cookies = append(cookies, ck)
		}
	}
	return cookies
}

func TestSessionCookie(t *testing.T) {
	router, countKeys := setupRouter(t)
	c := newClient(t, router)

	// a request without cookie gets one, a request carrying one isn't issued a new one
	resp := c.send(http.MethodGet, "/v2/")
	require.Len(t, sessionCookies(resp), 1)
	c.receive(resp)
	anonymous := c.cookies[config.SessionCookieName].Value
	for i := 0; i < 3; i++ {
		resp = c.send(http.MethodGet, "/v2/")
		assert.Empty(t, sessionCookies(resp))
		c.receive(resp)
	}
	assert.Equal(t, anonymous, c.cookies[config.SessionCookieName].Value)
	assert.Equal(t, 0, countKeys())

	// a session acquiring data gets its cookie
	resp = c.send(http.MethodGet, "/c/oidc/login")
	cookies := sessionCookies(resp)
	require.Len(t, cookies, 1)
	assert.NotEqual(t, anonymous, cookies[0].Value)
	exist, _ := harborpder.SessionExist(context.Background(), cookies[0].Value)
	assert.True(t, exist)
	c.receive(resp)
	assert.Equal(t, "random", c.do(http.MethodGet, "/c/oidc/callback"))

	// login regenerates the session ID
	stateSID := c.cookies[config.SessionCookieName].Value
	resp = c.send(http.MethodPost, "/c/login")
	cookies = sessionCookies(resp)
	require.Len(t, cookies, 1)
	assert.NotEqual(t, stateSID, cookies[0].Value)
	c.receive(resp)
	assert.Equal(t, "admin", c.do(http.MethodGet, "/v2/"))
	assert.Equal(t, 1, countKeys())

	// logout deletes the cookie
	resp = c.send(http.MethodGet, "/c/log_out")
	cookies = sessionCookies(resp)
	require.Len(t, cookies, 1)
	assert.Negative(t, cookies[0].MaxAge)
	c.receive(resp)
	assert.Nil(t, c.cookies[config.SessionCookieName])
	assert.Equal(t, 0, countKeys())
}

func TestDelayedAnonymousResponse(t *testing.T) {
	router, _ := setupRouter(t)
	c := newClient(t, router)
	c.do(http.MethodGet, "/v2/")

	// the response of an anonymous request arrives after the login
	delayed := c.send(http.MethodGet, "/v2/")
	c.do(http.MethodPost, "/c/login")
	c.receive(delayed)
	assert.Equal(t, "admin", c.do(http.MethodGet, "/v2/"))

	c.do(http.MethodGet, "/c/log_out")
	c.do(http.MethodGet, "/v2/")
	// and after the OIDC redirect
	delayed = c.send(http.MethodGet, "/v2/")
	c.do(http.MethodGet, "/c/oidc/login")
	c.receive(delayed)
	assert.Equal(t, "random", c.do(http.MethodGet, "/c/oidc/callback"))
}
