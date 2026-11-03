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

package middlewares

import (
	"github.com/goharbor/harbor/src/common"
	"github.com/goharbor/harbor/src/lib/config"
	_ "github.com/goharbor/harbor/src/pkg/config/inmemory"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_readonlySkipper(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"login", args{httptest.NewRequest(http.MethodPost, "/c/login", nil)}, true},
		{"login get", args{httptest.NewRequest(http.MethodGet, "/c/login", nil)}, false},
		{"onboard", args{httptest.NewRequest(http.MethodPost, "/c/oidc/onboard", nil)}, true},
		{"user exist", args{httptest.NewRequest(http.MethodPost, "/c/userExists", nil)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pass bool
			for _, skipper := range readonlySkippers {
				if got := skipper(tt.args.r); got == tt.want {
					pass = true
				}
			}
			if !pass {
				t.Errorf("readonlySkippers() = %v, want %v", tt.args, tt.want)
			}
		})
	}
}

func TestPushTransactionSkippers(t *testing.T) {
	old := config.DefaultCfgManager
	defer func() { config.DefaultCfgManager = old }()
	for _, provider := range []string{"db", "redis"} {
		config.InitWithSettings(map[string]any{common.QuotaUpdateProvider: provider})
		for _, route := range []struct{ method, path string }{
			{http.MethodPut, "/v2/library/test/manifests/latest"},
			{http.MethodPut, "/v2/library/test/manifests/sha256:abc"},
			{http.MethodPost, "/v2/library/test/blobs/uploads/"},
		} {
			req := httptest.NewRequest(route.method, route.path, nil)
			skipped := false
			for _, skipper := range dbTxSkippers {
				skipped = skipped || skipper(req)
			}
			require.Equal(t, provider == "redis", skipped, "%s %s provider=%s", route.method, route.path, provider)
		}
		req := httptest.NewRequest(http.MethodDelete, "/v2/library/test/manifests/sha256:abc", nil)
		for _, skipper := range dbTxSkippers {
			require.False(t, skipper(req))
		}
	}
}
