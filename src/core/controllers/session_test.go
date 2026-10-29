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

package controllers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/beego/beego/v2/server/web"
	beegosession "github.com/beego/beego/v2/server/web/session"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goharbor/harbor/src/common"
	_ "github.com/goharbor/harbor/src/core/auth/db"
	"github.com/goharbor/harbor/src/core/middlewares"
	"github.com/goharbor/harbor/src/core/session"
	"github.com/goharbor/harbor/src/lib/config"
	sessionmw "github.com/goharbor/harbor/src/server/middleware/session"
	v2handler "github.com/goharbor/harbor/src/server/v2.0/handler"
)

const (
	sessionRedis     = "127.0.0.1:6379"
	sessionIDPrefix  = "harbor-controllers-test-"
	csrfTokenHeader  = "X-Harbor-CSRF-Token"
	currentUserPath  = "/api/v2.0/users/current"
	sessionTTLMargin = 5 * time.Second
)

// browser keeps the cookies and the CSRF token like the portal does, the responses
// are applied explicitly so that the order in which they arrive can be controlled
type browser struct {
	t       *testing.T
	handler http.Handler
	cookies map[string]*http.Cookie
	token   string
}

func (b *browser) send(method, path string, body string, header http.Header) *http.Response {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set(csrfTokenHeader, b.token)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	for _, c := range b.cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	b.handler.ServeHTTP(rec, req)
	return rec.Result()
}

func (b *browser) receive(resp *http.Response) {
	for _, c := range resp.Cookies() {
		if c.MaxAge < 0 {
			delete(b.cookies, c.Name)
		} else {
			b.cookies[c.Name] = c
		}
	}
	if t := resp.Header.Get(csrfTokenHeader); t != "" {
		b.token = t
	}
}

func (b *browser) do(method, path string, body string, header http.Header) (int, string) {
	resp := b.send(method, path, body, header)
	b.receive(resp)
	data, err := io.ReadAll(resp.Body)
	require.NoError(b.t, err)
	return resp.StatusCode, string(data)
}

func (b *browser) sid() string {
	if c, ok := b.cookies[config.SessionCookieName]; ok {
		return c.Value
	}
	return ""
}

func setupSessionTest(t *testing.T, settings map[string]any) (*browser, *goredis.Client) {
	config.InitWithSettings(settings)

	original := web.GlobalSessions
	var err error
	web.GlobalSessions, err = beegosession.NewManager(session.HarborProviderName, &beegosession.ManagerConfig{
		CookieName:      config.SessionCookieName,
		Gclifetime:      3600,
		ProviderConfig:  "redis://" + sessionRedis + "/0",
		EnableSetCookie: true,
		SessionIDPrefix: sessionIDPrefix,
	})
	require.NoError(t, err)

	rdb := goredis.NewClient(&goredis.Options{Addr: sessionRedis})
	t.Cleanup(func() {
		web.GlobalSessions = original
		ctx := context.Background()
		keys, _ := rdb.Keys(ctx, sessionIDPrefix+"*").Result()
		for _, k := range keys {
			rdb.Del(ctx, k)
		}
		rdb.Close()
	})

	router := web.NewControllerRegister()
	router.Handler("/api/v2.0/*", v2handler.New())
	router.Add("/c/login", &CommonController{}, web.WithRouterMethods(&CommonController{}, "post:Login"))
	router.Add("/c/log_out", &CommonController{}, web.WithRouterMethods(&CommonController{}, "get:LogOut"))
	router.Add("/c/oidc/login", &OIDCController{}, web.WithRouterMethods(&OIDCController{}, "get:RedirectLogin"))
	router.Add("/c/oidc/callback", &OIDCController{}, web.WithRouterMethods(&OIDCController{}, "get:Callback"))
	router.InsertFilterChain("*", session.CookieFilterChain)
	router.Init()

	handler := http.Handler(router)
	mws := middlewares.MiddleWares()
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return &browser{t: t, handler: handler, cookies: map[string]*http.Cookie{}}, rdb
}

func TestLoginSession(t *testing.T) {
	b, rdb := setupSessionTest(t, map[string]any{
		common.AUTHMode:       common.DBAuth,
		common.SessionTimeout: 1,
		common.ExtEndpoint:    "http://harbor.test",
	})
	ctx := context.Background()
	sessionTimeout := time.Minute

	// the portal bootstraps with anonymous API calls, the session cookie
	// they get makes the following requests be served CSRF tokens
	code, _ := b.do(http.MethodGet, currentUserPath, "", nil)
	assert.Equal(t, http.StatusUnauthorized, code)
	anonymous := b.sid()
	require.NotEmpty(t, anonymous)
	code, _ = b.do(http.MethodGet, currentUserPath, "", nil)
	assert.Equal(t, http.StatusUnauthorized, code)
	require.NotEmpty(t, b.token)
	assert.Equal(t, anonymous, b.sid(), "anonymous session cookie should not be replaced")

	// the response of an anonymous request arrives after the login
	delayed := b.send(http.MethodGet, currentUserPath, "", nil)
	code, body := b.do(http.MethodPost, "/c/login", "principal=admin&password="+url.QueryEscape(os.Getenv("HARBOR_ADMIN_PASSWD")), nil)
	require.Equal(t, http.StatusOK, code, body)
	authenticated := b.sid()
	assert.NotEqual(t, anonymous, authenticated)
	b.receive(delayed)
	assert.Equal(t, authenticated, b.sid())
	code, body = b.do(http.MethodGet, currentUserPath, "", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"username":"admin"`)

	// the background requests don't renew the session
	require.NoError(t, rdb.Expire(ctx, authenticated, 10*time.Second).Err())
	code, _ = b.do(http.MethodGet, currentUserPath, "", http.Header{sessionmw.HeaderNoSessionRenewal: {"true"}})
	assert.Equal(t, http.StatusOK, code)
	assert.LessOrEqual(t, rdb.TTL(ctx, authenticated).Val(), 10*time.Second)
	code, _ = b.do(http.MethodGet, currentUserPath, "", nil)
	assert.Equal(t, http.StatusOK, code)
	assert.Greater(t, rdb.TTL(ctx, authenticated).Val(), sessionTimeout-sessionTTLMargin)

	code, _ = b.do(http.MethodGet, "/c/log_out", "", nil)
	assert.Equal(t, http.StatusOK, code)
	assert.Empty(t, b.sid())
	assert.Zero(t, rdb.Exists(ctx, authenticated).Val())
	code, _ = b.do(http.MethodGet, currentUserPath, "", nil)
	assert.Equal(t, http.StatusUnauthorized, code)
}

func TestOIDCLoginSession(t *testing.T) {
	var issuer string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"issuer":%q,"authorization_endpoint":%q,"token_endpoint":%q,"jwks_uri":%q,`+
			`"response_types_supported":["code"],"subject_types_supported":["public"],"id_token_signing_alg_values_supported":["RS256"]}`,
			issuer, issuer+"/auth", issuer+"/token", issuer+"/keys")
	}))
	defer provider.Close()
	issuer = provider.URL

	b, _ := setupSessionTest(t, map[string]any{
		common.AUTHMode:         common.OIDCAuth,
		common.SessionTimeout:   1,
		common.ExtEndpoint:      "http://harbor.test",
		common.OIDCEndpoint:     issuer,
		common.OIDCCLientID:     "harbor",
		common.OIDCClientSecret: "secret",
		common.OIDCScope:        "openid",
		common.OIDCVerifyCert:   false,
	})

	b.do(http.MethodGet, currentUserPath, "", nil)
	require.NotEmpty(t, b.sid())

	// the response of an anonymous request arrives after the redirection to the OIDC provider
	delayed := b.send(http.MethodGet, currentUserPath, "", nil)
	resp := b.send(http.MethodGet, "/c/oidc/login?redirect_url=%2Fharbor%2Fprojects", "", nil)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	b.receive(resp)
	b.receive(delayed)

	location, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	state := location.Query().Get("state")
	require.NotEmpty(t, state)
	// stop at the state check by having the provider deny the login
	code, body := b.do(http.MethodGet, "/c/oidc/callback?error=access_denied&state="+url.QueryEscape(state), "", nil)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.NotContains(t, body, "State mismatch")
	assert.Contains(t, body, "access_denied")
}
