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

// Package finalization drives the dag-finalization loop for iguazu-coordinator.
// It subscribes to the coordinator's CoordinatorInternal gRPC service, receives
// new-head events, calls gwat dag_finalize for each head, and reports the
// result back to the coordinator.
package finalization

import (
	"context"
	"fmt"
	"math/big"
	"time"

	coordpb "github.com/LFDT-Iguazu/iguazu-coordinator/proto/coordpb"
	dagTypes "github.com/LFDT-Iguazu/iguazu-types/blockdag/types"
	"github.com/LFDT-Iguazu/iguazu-types/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var log = logrus.WithField("prefix", "finalization")

// Service drives the dag-finalization loop.
type Service struct {
	coord coordpb.CoordinatorInternalClient
	gwat  *GwatClient

	// retryDelay controls how long we wait before reconnecting after an error.
	retryDelay time.Duration
}

// NewService creates a finalization Service. coordConn is the gRPC connection
// to the coordinator node; gwat is the initialised gwat JSON-RPC client.
func NewService(coordConn grpc.ClientConnInterface, gwat *GwatClient) *Service {
	return &Service{
		coord:      coordpb.NewCoordinatorInternalClient(coordConn),
		gwat:       gwat,
		retryDelay: 5 * time.Second,
	}
}

// Run starts the finalization loop. It blocks until ctx is cancelled.
// On any error it logs and retries after retryDelay.
func (s *Service) Run(ctx context.Context) {
	for {
		if err := s.run(ctx); err != nil {
			if ctx.Err() != nil {
				log.Info("Finalization service stopped")
				return
			}
			log.WithError(err).Error("Finalization loop error — retrying")
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.retryDelay):
			}
		}
	}
}

// run is a single iteration of the outer retry loop.
func (s *Service) run(ctx context.Context) error {
	stream, err := s.coord.StreamNewHeads(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.Wrap(err, "StreamNewHeads")
	}

	log.Info("Finalization: subscribed to StreamNewHeads")

	for {
		head, err := stream.Recv()
		if err != nil {
			return errors.Wrap(err, "stream.Recv")
		}
		if err := s.processHead(ctx, head); err != nil {
			return errors.Wrap(err, "processHead")
		}
	}
}

// processHead runs the full finalization cycle for one new head.
func (s *Service) processHead(ctx context.Context, head *coordpb.NewHeadEvent) error {
	log.WithFields(logrus.Fields{
		"slot":      head.GetSlot(),
		"blockRoot": fmt.Sprintf("%#x", head.GetBlockRoot()),
	}).Info("Finalization: new head")

	paramsResp, err := s.coord.GetFinalizationParams(ctx, &coordpb.GetFinalizationParamsRequest{
		BlockRoot: head.GetBlockRoot(),
		SyncMode:  uint32(dagTypes.NoSync),
	})
	if err != nil {
		return errors.Wrap(err, "GetFinalizationParams")
	}

	fp := protoToFinalizationParams(paramsResp)

	log.WithFields(logrus.Fields{
		"spines":    fp.Spines,
		"baseSpine": fp.BaseSpine,
		"cp.Epoch":  fp.Checkpoint.Epoch,
		"syncMode":  fp.SyncMode,
	}).Info("Finalization: calling dag_finalize")

	finRes, err := s.gwat.ExecutionDagFinalize(ctx, fp)
	if err != nil || finRes.LFSpine == nil {
		return errors.Wrap(err, "ExecutionDagFinalize")
	}

	req := buildSubmitRequest(finRes, paramsResp.GetCheckpoint())
	if _, err := s.coord.SubmitFinalizationResult(ctx, req); err != nil {
		return errors.Wrap(err, "SubmitFinalizationResult")
	}

	log.WithFields(logrus.Fields{
		"lfSpine": finRes.LFSpine.Hex(),
		"cpEpoch": finRes.CpEpoch,
		"cpRoot":  finRes.CpRoot,
	}).Info("Finalization: success")

	return nil
}

// protoToFinalizationParams converts the gRPC response to the dag_finalize params.
func protoToFinalizationParams(r *coordpb.FinalizationParamsResponse) *dagTypes.FinalizationParams {
	fp := &dagTypes.FinalizationParams{
		SyncMode: dagTypes.SyncMode(r.GetSyncMode()),
	}

	for _, raw := range r.GetSpines() {
		fp.Spines = append(fp.Spines, common.BytesToHash(raw))
	}

	if len(r.GetBaseSpine()) > 0 {
		h := common.BytesToHash(r.GetBaseSpine())
		fp.BaseSpine = &h
	}

	if cp := r.GetCheckpoint(); cp != nil {
		root := common.BytesToHash(cp.GetRoot())
		spine := common.BytesToHash(cp.GetSpine())
		fp.Checkpoint = &dagTypes.Checkpoint{
			FinEpoch: cp.GetFinEpoch(),
			Epoch:    cp.GetEpoch(),
			Root:     root,
			Spine:    spine,
		}
	}

	for _, op := range r.GetValSyncData() {
		vs := &dagTypes.ValidatorSync{
			OpType:          dagTypes.ValidatorSyncOp(op.GetOpType()),
			ProcEpoch:       op.GetProcEpoch(),
			Index:           op.GetIndex(),
			Creator:         common.BytesToAddress(op.GetCreatorAddress()),
			InitTxHash:      common.BytesToHash(op.GetInitTxHash()),
			ActivationEpoch: op.GetActivationEpoch(),
			ExitEpoch:       op.GetExitEpoch(),
		}
		if raw := op.GetAmountWei(); len(raw) > 0 {
			vs.Amount = new(big.Int).SetBytes(raw)
		}
		if raw := op.GetBalanceWei(); len(raw) > 0 {
			vs.Balance = new(big.Int).SetBytes(raw)
		}
		fp.ValSyncData = append(fp.ValSyncData, vs)
	}

	return fp
}

// buildSubmitRequest constructs the SubmitFinalizationResultRequest from the
// gwat dag_finalize result and the original param checkpoint.
func buildSubmitRequest(res *dagTypes.FinalizationResult, paramCp *coordpb.FinalizationCheckpoint) *coordpb.SubmitFinalizationResultRequest {
	req := &coordpb.SubmitFinalizationResultRequest{
		ParamCheckpoint: paramCp,
	}
	if res.LFSpine != nil {
		req.LfSpine = res.LFSpine.Bytes()
	}
	if res.CpEpoch != nil {
		req.ResultCpEpoch = *res.CpEpoch
	}
	if res.CpRoot != nil {
		req.ResultCpRoot = res.CpRoot.Bytes()
	}
	return req
}
