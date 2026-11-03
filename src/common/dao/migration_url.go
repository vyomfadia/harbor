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
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/goharbor/harbor/src/common/models"
)

// migrationDatabaseURL selects a direct connection for migrations without
// changing the runtime ORM connection. Both schema checks and upgrades use it.
func migrationDatabaseURL(database *models.PostGreSQL, override string) (string, error) {
	if override != "" {
		u, err := url.Parse(override)
		// Do not include the URL or parse error: either may contain credentials.
		if err != nil {
			return "", fmt.Errorf("invalid POSTGRESQL_MIGRATION_URL")
		}
		switch u.Scheme {
		case "postgres", "postgresql", "pgx5":
		default:
			return "", fmt.Errorf("POSTGRESQL_MIGRATION_URL must use postgres, postgresql or pgx5 scheme")
		}
		if u.Hostname() == "" || u.Path == "" || u.Path == "/" || u.Fragment != "" {
			return "", fmt.Errorf("POSTGRESQL_MIGRATION_URL must specify a host and database, without a fragment")
		}
		// golang-migrate selects the driver by scheme; Harbor uses the pgx v5 driver.
		u.Scheme = "pgx5"
		return u.String(), nil
	}
	u := url.URL{
		Scheme:   "pgx5",
		User:     url.UserPassword(database.Username, database.Password),
		Host:     net.JoinHostPort(database.Host, strconv.Itoa(database.Port)),
		Path:     database.Database,
		RawQuery: url.Values{"sslmode": {database.SSLMode}}.Encode(),
	}
	return u.String(), nil
}
