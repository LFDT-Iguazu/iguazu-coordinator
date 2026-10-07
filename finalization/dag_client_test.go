// Copyright 2026 Digital Clever Solution LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package finalization

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	dagTypes "github.com/LFDT-Iguazu/iguazu-types/blockdag/types"
	"github.com/LFDT-Iguazu/iguazu-types/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestGwat starts an HTTP server that records the decoded JSON-RPC request
// and replies with status and body.
func newTestGwat(t *testing.T, status int, body string, got *map[string]json.RawMessage) *GwatClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, contentType, r.Header.Get("Content-Type"))
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if got != nil {
			require.NoError(t, json.Unmarshal(raw, got))
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	c, err := NewGwatClient(srv.URL)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestNewGwatClient_RejectsNonHTTPScheme(t *testing.T) {
	_, err := NewGwatClient("ws://localhost:8546")
	require.Error(t, err)
	_, err = NewGwatClient("/tmp/gwat.ipc")
	require.Error(t, err)
}

func TestExecutionDagFinalize_Success(t *testing.T) {
	var got map[string]json.RawMessage
	c := newTestGwat(t, http.StatusOK,
		`{"jsonrpc":"2.0","id":1,"result":{"error":null,"lfSpine":"0x0000000000000000000000000000000000000000000000000000000000000011","cpEpoch":7,"cpRoot":"0x0000000000000000000000000000000000000000000000000000000000000022"}}`,
		&got)

	base := common.HexToHash("0x33")
	params := &dagTypes.FinalizationParams{
		Spines:    common.HashArray{common.HexToHash("0x44")},
		BaseSpine: &base,
		SyncMode:  dagTypes.HeadSync,
	}
	res, err := c.ExecutionDagFinalize(context.Background(), params)
	require.NoError(t, err)

	assert.JSONEq(t, `"2.0"`, string(got["jsonrpc"]))
	assert.JSONEq(t, `"dag_finalize"`, string(got["method"]))
	wantParams, err := json.Marshal([]interface{}{params})
	require.NoError(t, err)
	assert.JSONEq(t, string(wantParams), string(got["params"]))

	require.NotNil(t, res.LFSpine)
	assert.Equal(t, common.HexToHash("0x11"), *res.LFSpine)
	require.NotNil(t, res.CpEpoch)
	assert.Equal(t, uint64(7), *res.CpEpoch)
	require.NotNil(t, res.CpRoot)
	assert.Equal(t, common.HexToHash("0x22"), *res.CpRoot)
}

func TestExecutionDagFinalize_ResultError(t *testing.T) {
	c := newTestGwat(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":{"error":"boom"}}`, nil)
	res, err := c.ExecutionDagFinalize(context.Background(), &dagTypes.FinalizationParams{})
	require.ErrorContains(t, err, "dag_finalize: boom")
	require.NotNil(t, res)
}

func TestExecutionDagFinalize_RPCError(t *testing.T) {
	c := newTestGwat(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`, nil)
	res, err := c.ExecutionDagFinalize(context.Background(), &dagTypes.FinalizationParams{})
	require.ErrorContains(t, err, "method not found")
	assert.Nil(t, res)
}

func TestExecutionDagFinalize_NoResult(t *testing.T) {
	c := newTestGwat(t, http.StatusOK, `{"jsonrpc":"2.0","id":1}`, nil)
	_, err := c.ExecutionDagFinalize(context.Background(), &dagTypes.FinalizationParams{})
	require.ErrorIs(t, err, ErrNoResult)
}

func TestExecutionDagFinalize_HTTPStatusError(t *testing.T) {
	c := newTestGwat(t, http.StatusServiceUnavailable, "unavailable", nil)
	_, err := c.ExecutionDagFinalize(context.Background(), &dagTypes.FinalizationParams{})
	require.ErrorContains(t, err, "503")
}

func TestExecutionDagCoordinatedState_SendsEmptyParams(t *testing.T) {
	var got map[string]json.RawMessage
	c := newTestGwat(t, http.StatusOK, `{"jsonrpc":"2.0","id":1,"result":{"cpEpoch":3}}`, &got)
	res, err := c.ExecutionDagCoordinatedState(context.Background())
	require.NoError(t, err)

	assert.JSONEq(t, `"dag_coordinatedState"`, string(got["method"]))
	assert.JSONEq(t, `[]`, string(got["params"]))
	require.NotNil(t, res.CpEpoch)
	assert.Equal(t, uint64(3), *res.CpEpoch)
}
