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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync/atomic"

	dagTypes "github.com/LFDT-Iguazu/iguazu-types/blockdag/types"
	"github.com/pkg/errors"
)

const (
	methodDagFinalize         = "dag_finalize"
	methodDagCoordinatedState = "dag_coordinatedState"

	contentType = "application/json"
	// maxResponseSize bounds the JSON-RPC response body read from gwat.
	maxResponseSize = 5 * 1024 * 1024
)

// ErrNoResult is returned when a JSON-RPC response has neither a result nor an error.
var ErrNoResult = errors.New("no result in JSON-RPC response")

// GwatClient is a minimal JSON-RPC 2.0 over HTTP client for the gwat execution
// layer. It exposes only the dag methods needed by the finalization service and
// talks to gwat purely over the wire, without linking any gwat code.
type GwatClient struct {
	endpoint string
	http     *http.Client
	nextID   atomic.Uint64
}

type jsonrpcRequest struct {
	Version string        `json:"jsonrpc"`
	ID      uint64        `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type jsonrpcResponse struct {
	Version string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonrpcError   `json:"error"`
}

type jsonrpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *jsonrpcError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("json-rpc error %d", e.Code)
	}
	return e.Message
}

// NewGwatClient validates the gwat HTTP endpoint and returns a ready client.
func NewGwatClient(endpoint string) (*GwatClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "dial gwat endpoint")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.Errorf("dial gwat endpoint: unsupported scheme %q, want http or https", u.Scheme)
	}
	return &GwatClient{endpoint: endpoint, http: new(http.Client)}, nil
}

// Close releases idle connections.
func (c *GwatClient) Close() {
	c.http.CloseIdleConnections()
}

// call performs a single JSON-RPC request and decodes the result into result.
func (c *GwatClient) call(ctx context.Context, result interface{}, method string, params ...interface{}) error {
	if params == nil {
		params = []interface{}{}
	}
	body, err := json.Marshal(&jsonrpcRequest{
		Version: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", contentType)
	req.Header.Set("Content-Type", contentType)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.Errorf("%s: %s", resp.Status, bytes.TrimSpace(raw))
	}

	var msg jsonrpcResponse
	if err := json.Unmarshal(raw, &msg); err != nil {
		return errors.Wrap(err, "decode JSON-RPC response")
	}
	switch {
	case msg.Error != nil:
		return msg.Error
	case len(msg.Result) == 0:
		return ErrNoResult
	default:
		return json.Unmarshal(msg.Result, result)
	}
}

// ExecutionDagFinalize calls dag_finalize on gwat.
func (c *GwatClient) ExecutionDagFinalize(ctx context.Context, params *dagTypes.FinalizationParams) (*dagTypes.FinalizationResult, error) {
	result := &dagTypes.FinalizationResult{}
	if err := c.call(ctx, result, methodDagFinalize, params); err != nil {
		return nil, errors.Wrap(err, "dag_finalize RPC")
	}
	if result.Error != nil {
		return result, fmt.Errorf("dag_finalize: %s", *result.Error)
	}
	return result, nil
}

// ExecutionDagCoordinatedState calls dag_coordinatedState on gwat to retrieve
// the current gwat coordinated checkpoint.
func (c *GwatClient) ExecutionDagCoordinatedState(ctx context.Context) (*dagTypes.FinalizationResult, error) {
	result := &dagTypes.FinalizationResult{}
	if err := c.call(ctx, result, methodDagCoordinatedState); err != nil {
		return nil, errors.Wrap(err, "dag_coordinatedState RPC")
	}
	if result.Error != nil {
		return result, fmt.Errorf("dag_coordinatedState: %s", *result.Error)
	}
	return result, nil
}
