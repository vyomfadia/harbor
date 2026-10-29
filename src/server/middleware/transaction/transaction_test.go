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

package transaction

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	o "github.com/beego/beego/v2/client/orm"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"github.com/goharbor/harbor/src/controller/event/metadata"
	"github.com/goharbor/harbor/src/lib/errors"
	lib_http "github.com/goharbor/harbor/src/lib/http"
	"github.com/goharbor/harbor/src/lib/orm"
	"github.com/goharbor/harbor/src/pkg/notification"
	"github.com/goharbor/harbor/src/server/middleware"
)

type mockOrmer struct {
	o.Ormer
	tx       mockTxOrmer
	beginErr error
}

func (m *mockOrmer) Begin() (o.TxOrmer, error) {
	return &m.tx, m.beginErr
}

func (m *mockOrmer) Reset() {
	m.tx.Reset()
	m.beginErr = nil
}

type mockTxOrmer struct {
	o.TxOrmer
	commitErr error
	records   []any
}

func (m *mockTxOrmer) Insert(i any) (int64, error) {
	m.records = append(m.records, i)
	return int64(len(m.records)), nil
}

func (m *mockTxOrmer) Commit() error {
	return m.commitErr
}

func (m *mockTxOrmer) Rollback() error {
	m.ResetRecords()
	return nil
}

func (m *mockTxOrmer) ResetRecords() {
	m.records = nil
}

func (m *mockTxOrmer) Reset() {
	m.ResetRecords()
	m.commitErr = nil
}

func TestTransaction(t *testing.T) {
	assert := assert.New(t)

	tx := mockTxOrmer{}
	mo := &mockOrmer{
		tx: tx,
	}

	newRequest := func(method, target string, body io.Reader) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/req1", nil)
		return req.WithContext(orm.NewContext(req.Context(), mo))
	}

	next := func(status int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mo.tx.Insert("record1")
			w.WriteHeader(status)
		})
	}

	// test response status code accepted
	req1 := newRequest(http.MethodGet, "/req", nil)
	rec1 := httptest.NewRecorder()
	Middleware()(next(http.StatusOK)).ServeHTTP(rec1, req1)
	assert.Equal(http.StatusOK, rec1.Code)
	assert.NotEmpty(mo.tx.records)

	mo.tx.ResetRecords()
	assert.Empty(mo.tx.records)

	// test response status code not accepted
	req2 := newRequest(http.MethodGet, "/req", nil)
	rec2 := httptest.NewRecorder()
	Middleware()(next(http.StatusBadRequest)).ServeHTTP(rec2, req2)
	assert.Equal(http.StatusBadRequest, rec2.Code)
	assert.Empty(mo.tx.records)

	// test begin transaction failed
	mo.beginErr = errors.New("begin tx failed")
	req3 := newRequest(http.MethodGet, "/req", nil)
	rec3 := httptest.NewRecorder()
	Middleware()(next(http.StatusBadRequest)).ServeHTTP(rec3, req3)
	assert.Equal(http.StatusInternalServerError, rec3.Code)
	assert.Empty(mo.tx.records)

	// test commit transaction failed
	mo.beginErr = nil
	mo.tx.commitErr = errors.New("commit tx failed")
	req4 := newRequest(http.MethodGet, "/req", nil)
	rec4 := httptest.NewRecorder()
	Middleware()(next(http.StatusOK)).ServeHTTP(rec4, req4)
	assert.Equal(http.StatusInternalServerError, rec4.Code)

	// test MustCommit
	mo.Reset()
	assert.Empty(mo.tx.records)

	txMustCommit := func(status int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer MustCommit(r)
			mo.tx.Insert("record1")
			w.WriteHeader(status)
		})
	}

	req5 := newRequest(http.MethodGet, "/req", nil)
	rec5 := httptest.NewRecorder()

	m1 := middleware.New(func(w http.ResponseWriter, r *http.Request, next http.Handler) {
		type key struct{}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), key{}, "value")))
	})

	Middleware()(m1((txMustCommit(http.StatusBadRequest)))).ServeHTTP(rec5, req5)
	assert.Equal(http.StatusBadRequest, rec2.Code)
	assert.NotEmpty(mo.tx.records)
}

func TestMustCommit(t *testing.T) {
	newRequest := func(ctx context.Context) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/req", nil)
		return req.WithContext(ctx)
	}

	ctx := context.Background()
	committableCtx := context.WithValue(ctx, orm.CommittedKey{}, new(bool))

	type args struct {
		r *http.Request
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"request committable", args{newRequest(committableCtx)}, false},
		{"request not committable", args{newRequest(ctx)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MustCommit(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("MustCommit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRetryableMiddleware(t *testing.T) {
	deadlock := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

	type result struct {
		code   int
		body   string
		calls  int
		events []string
	}

	// serve runs the handler which fails with the errors in order and then
	// succeeds, every attempt inserts a record and adds a notification event
	serve := func(mo *mockOrmer, errs ...error) result {
		calls := 0
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls++
			o, err := orm.FromContext(r.Context())
			assert.NoError(t, err)
			_, _ = o.Insert(calls)
			notification.AddEvent(r.Context(), &metadata.PushArtifactEventMetadata{Tag: fmt.Sprintf("attempt-%d", calls)})
			if calls <= len(errs) {
				lib_http.SendError(w, errs[calls-1])
				return
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(fmt.Sprintf("attempt-%d", calls)))
		})

		evc := notification.NewEventCtx()
		req := httptest.NewRequest(http.MethodPut, "/v2/library/hello-world/manifests/latest", nil)
		req = req.WithContext(notification.NewContext(orm.NewContext(req.Context(), mo), evc))
		rec := httptest.NewRecorder()
		RetryableMiddleware("tx-test")(h).ServeHTTP(rec, req)

		res := result{code: rec.Code, body: rec.Body.String(), calls: calls}
		for e := evc.Events.Front(); e != nil; e = e.Next() {
			res.events = append(res.events, e.Value.(*metadata.PushArtifactEventMetadata).Tag)
		}
		return res
	}

	t.Run("success", func(t *testing.T) {
		mo := &mockOrmer{}
		res := serve(mo)
		assert.Equal(t, http.StatusCreated, res.code)
		assert.Equal(t, "attempt-1", res.body)
		assert.Equal(t, 1, res.calls)
		assert.Equal(t, []string{"attempt-1"}, res.events)
		assert.Equal(t, []any{1}, mo.tx.records)
	})

	t.Run("retry on deadlock", func(t *testing.T) {
		mo := &mockOrmer{}
		res := serve(mo, errors.Wrap(deadlock, "associate blob with project failed"))
		assert.Equal(t, http.StatusCreated, res.code)
		// the response, the records and the events of the aborted attempt are dropped
		assert.Equal(t, "attempt-2", res.body)
		assert.Equal(t, 2, res.calls)
		assert.Equal(t, []string{"attempt-2"}, res.events)
		assert.Equal(t, []any{2}, mo.tx.records)
	})

	t.Run("no retry on other errors", func(t *testing.T) {
		mo := &mockOrmer{}
		res := serve(mo, errors.BadRequestError(nil).WithMessage("bad request"))
		assert.Equal(t, http.StatusBadRequest, res.code)
		assert.Contains(t, res.body, "bad request")
		assert.Equal(t, 1, res.calls)
		assert.Equal(t, []string{"attempt-1"}, res.events)
		assert.Empty(t, mo.tx.records)
	})

	t.Run("give up", func(t *testing.T) {
		mo := &mockOrmer{}
		res := serve(mo, deadlock, deadlock, deadlock, deadlock, deadlock, deadlock, deadlock, deadlock)
		assert.Equal(t, http.StatusInternalServerError, res.code)
		assert.Less(t, res.calls, 8)
		assert.Greater(t, res.calls, 1)
		assert.Len(t, res.events, 1)
		assert.Empty(t, mo.tx.records)
	})

	t.Run("commit failed", func(t *testing.T) {
		mo := &mockOrmer{tx: mockTxOrmer{commitErr: errors.New("commit tx failed")}}
		res := serve(mo)
		assert.Equal(t, http.StatusInternalServerError, res.code)
		assert.NotContains(t, res.body, "attempt-1")
		assert.Equal(t, 1, res.calls)
	})

	t.Run("begin failed", func(t *testing.T) {
		mo := &mockOrmer{beginErr: errors.New("begin tx failed")}
		res := serve(mo)
		assert.Equal(t, http.StatusInternalServerError, res.code)
		assert.Equal(t, 0, res.calls)
	})
}
