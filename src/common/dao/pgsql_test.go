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
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/beego/beego/v2/client/orm"
	"github.com/goharbor/harbor/src/common/models"
)

func TestMaxOpenConns(t *testing.T) {
	var wg sync.WaitGroup

	queryNum := 200
	results := make([]bool, queryNum)
	for i := range queryNum {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			o := orm.NewOrm()
			if _, err := o.Raw("SELECT pg_sleep(10)").Exec(); err != nil {
				fmt.Printf("failed to get the count of the projects, error: %v\n", err)
				results[i] = false
			} else {
				results[i] = true
			}
		}(i)
	}

	wg.Wait()

	for _, success := range results {
		if !success {
			t.Fatal("max open conns not work")
		}
	}
}

// TestMigrationURLOverride uses the test database as the direct endpoint and an
// unreachable runtime endpoint to prove that both migration paths use the override.
func TestMigrationURLOverride(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("POSTGRESQL_PORT"))
	if err != nil {
		t.Fatal(err)
	}
	direct, err := migrationDatabaseURL(&models.PostGreSQL{
		Host: os.Getenv("POSTGRESQL_HOST"), Port: port,
		Username: os.Getenv("POSTGRESQL_USR"), Password: os.Getenv("POSTGRESQL_PWD"),
		Database: os.Getenv("POSTGRESQL_DATABASE"), SSLMode: "disable",
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("POSTGRESQL_MIGRATION_URL", direct)
	runtime := &models.PostGreSQL{Host: "127.0.0.1", Port: 1, Database: "unreachable", SSLMode: "disable"}
	m, err := NewMigrator(runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, _, err := m.Version(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeSchema(&models.Database{Type: "postgresql", PostGreSQL: runtime}); err != nil {
		t.Fatal(err)
	}
}
