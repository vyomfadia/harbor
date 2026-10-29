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
	"bufio"
	"net"
	"net/http"
	"net/url"

	"github.com/beego/beego/v2/server/web"
	beegoctx "github.com/beego/beego/v2/server/web/context"

	"github.com/goharbor/harbor/src/lib"
	"github.com/goharbor/harbor/src/lib/config"
)

// CookieFilterChain is a beego filter chain that keeps a request which already carries
// a session cookie from being issued a new one for a session that won't be persisted.
//
// Beego starts a session with a fresh ID whenever the cookie doesn't refer to a stored
// session, and the new, empty session is never stored. Re-issuing its cookie on every
// anonymous response lets a late one overwrite the cookie set by a concurrent login or
// OIDC redirect. Requests without a session cookie still get one, as carrying it is
// what makes the UI requests get CSRF tokens.
func CookieFilterChain(next web.FilterFunc) web.FilterFunc {
	return func(ctx *beegoctx.Context) {
		if !lib.GetCarrySession(ctx.Request.Context()) {
			next(ctx)
			return
		}
		w := &cookieWriter{ResponseWriter: ctx.ResponseWriter.ResponseWriter, ctx: ctx}
		ctx.ResponseWriter.ResponseWriter = w
		next(ctx)
		// nothing was written, the headers are sent once the handler returns
		w.dropUnusedCookies()
	}
}

type cookieWriter struct {
	http.ResponseWriter
	ctx     *beegoctx.Context
	checked bool
}

func (w *cookieWriter) WriteHeader(code int) {
	w.dropUnusedCookies()
	w.ResponseWriter.WriteHeader(code)
}

func (w *cookieWriter) Write(b []byte) (int, error) {
	w.dropUnusedCookies()
	return w.ResponseWriter.Write(b)
}

func (w *cookieWriter) Flush() {
	w.dropUnusedCookies()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *cookieWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.dropUnusedCookies()
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

func (w *cookieWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// dropUnusedCookies removes the session cookies other than the one of the current
// session, or all of them if the current session won't be persisted. Cookies deleting
// the session (logout) are kept.
func (w *cookieWriter) dropUnusedCookies() {
	if w.checked {
		return
	}
	w.checked = true

	var keep string
	switch s := w.ctx.Input.CruSession.(type) {
	case nil: // destroyed
	case *Store:
		s.lock.RLock()
		if s.persisted || len(s.values) > 0 {
			keep = url.QueryEscape(s.sid)
		}
		s.lock.RUnlock()
	default:
		return
	}

	h := w.Header()
	var cookies []string
	for _, v := range h.Values("Set-Cookie") {
		c, err := http.ParseSetCookie(v)
		if err == nil && c.Name == config.SessionCookieName && c.Value != "" && c.Value != keep {
			continue
		}
		cookies = append(cookies, v)
	}
	if len(cookies) == 0 {
		h.Del("Set-Cookie")
	} else {
		h["Set-Cookie"] = cookies
	}
}
