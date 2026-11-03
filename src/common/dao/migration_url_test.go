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
	"net/url"
	"strings"
	"testing"

	"github.com/goharbor/harbor/src/common/models"
)

func TestMigrationDatabaseURL(t *testing.T) {
	runtime := &models.PostGreSQL{Host: "::1", Port: 6432, Username: "runtime", Password: "p@ss:/?#", Database: "registry", SSLMode: "require"}
	before := *runtime
	t.Run("fallback", func(t *testing.T) {
		got, err := migrationDatabaseURL(runtime, "")
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Fatal(err)
		}
		password, _ := u.User.Password()
		if u.Scheme != "pgx5" || u.Host != "[::1]:6432" || u.User.Username() != "runtime" || password != runtime.Password || u.Path != "/registry" || u.Query().Get("sslmode") != "require" {
			t.Fatal("runtime connection settings were not preserved")
		}
	})
	for _, scheme := range []string{"postgres", "postgresql", "pgx5"} {
		t.Run(scheme, func(t *testing.T) {
			direct := scheme + "://migrator:p%40ss@direct:5432/registry?sslmode=verify-full&sslrootcert=%2Fcerts%2Fca.pem&connect_timeout=10"
			got, err := migrationDatabaseURL(runtime, direct)
			if err != nil {
				t.Fatal(err)
			}
			if got != "pgx5"+strings.TrimPrefix(direct, scheme) {
				t.Fatal("direct connection settings were not preserved")
			}
		})
	}
	for _, invalid := range []string{"postgres://migrator:secret%zz@direct/registry", "mysql://migrator:secret@direct/registry", "postgres:///registry", "postgres://direct", "postgres://direct/", "postgres://direct/registry#secret"} {
		t.Run("invalid", func(t *testing.T) {
			got, err := migrationDatabaseURL(runtime, invalid)
			if err == nil || got != "" {
				t.Fatal("invalid override must fail instead of falling back")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatal("error contains credentials")
			}
		})
	}
	if *runtime != before {
		t.Fatal("runtime configuration was modified")
	}
}
