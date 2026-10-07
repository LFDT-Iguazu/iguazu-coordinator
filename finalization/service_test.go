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
	"math/big"
	"testing"

	coordpb "github.com/LFDT-Iguazu/iguazu-coordinator/proto/coordpb"
	dagTypes "github.com/LFDT-Iguazu/iguazu-types/blockdag/types"
	"github.com/LFDT-Iguazu/iguazu-types/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- protoToFinalizationParams tests ----

func TestProtoToFinalizationParams_EmptyResponse(t *testing.T) {
	fp := protoToFinalizationParams(&coordpb.FinalizationParamsResponse{})
	assert.Empty(t, fp.Spines)
	assert.Nil(t, fp.BaseSpine)
	assert.Nil(t, fp.Checkpoint)
	assert.Empty(t, fp.ValSyncData)
}

func TestProtoToFinalizationParams_FullRoundTrip(t *testing.T) {
	spine1 := common.HexToHash("0x1111")
	spine2 := common.HexToHash("0x2222")
	base := common.HexToHash("0x3333")
	cpRoot := common.HexToHash("0x4444")
	cpSpine := common.HexToHash("0x5555")
	creator := common.HexToAddress("0xabcd")
	txHash := common.HexToHash("0x6666")
	amount := big.NewInt(1000)
	balance := big.NewInt(5000)

	resp := &coordpb.FinalizationParamsResponse{
		Spines:    [][]byte{spine1.Bytes(), spine2.Bytes()},
		BaseSpine: base.Bytes(),
		Checkpoint: &coordpb.FinalizationCheckpoint{
			FinEpoch: 3,
			Epoch:    4,
			Root:     cpRoot.Bytes(),
			Spine:    cpSpine.Bytes(),
		},
		ValSyncData: []*coordpb.ValidatorSyncOp{
			{
				OpType:          uint32(dagTypes.UpdateBalance),
				ProcEpoch:       5,
				Index:           42,
				CreatorAddress:  creator.Bytes(),
				AmountWei:       amount.Bytes(),
				InitTxHash:      txHash.Bytes(),
				ActivationEpoch: 2,
				ExitEpoch:       100,
				BalanceWei:      balance.Bytes(),
			},
		},
		SyncMode: uint32(dagTypes.HeadSync),
	}

	fp := protoToFinalizationParams(resp)

	assert.Equal(t, dagTypes.HeadSync, fp.SyncMode)
	require.Len(t, fp.Spines, 2)
	assert.Equal(t, spine1, fp.Spines[0])
	assert.Equal(t, spine2, fp.Spines[1])
	require.NotNil(t, fp.BaseSpine)
	assert.Equal(t, base, *fp.BaseSpine)

	require.NotNil(t, fp.Checkpoint)
	assert.Equal(t, uint64(3), fp.Checkpoint.FinEpoch)
	assert.Equal(t, uint64(4), fp.Checkpoint.Epoch)
	assert.Equal(t, cpRoot, fp.Checkpoint.Root)
	assert.Equal(t, cpSpine, fp.Checkpoint.Spine)

	require.Len(t, fp.ValSyncData, 1)
	op := fp.ValSyncData[0]
	assert.Equal(t, dagTypes.UpdateBalance, op.OpType)
	assert.Equal(t, uint64(5), op.ProcEpoch)
	assert.Equal(t, uint64(42), op.Index)
	assert.Equal(t, creator, op.Creator)
	assert.Equal(t, txHash, op.InitTxHash)
	assert.Equal(t, uint64(2), op.ActivationEpoch)
	assert.Equal(t, uint64(100), op.ExitEpoch)
	assert.Equal(t, amount, op.Amount)
	assert.Equal(t, balance, op.Balance)
}

// ---- buildSubmitRequest tests ----

func TestBuildSubmitRequest_WithResult(t *testing.T) {
	lfSpine := common.HexToHash("0xaaaa")
	cpRoot := common.HexToHash("0xbbbb")
	cpEpoch := uint64(7)
	paramCp := &coordpb.FinalizationCheckpoint{Epoch: 6, Root: common.HexToHash("0xcccc").Bytes()}

	res := &dagTypes.FinalizationResult{
		LFSpine: &lfSpine,
		CpEpoch: &cpEpoch,
		CpRoot:  &cpRoot,
	}

	req := buildSubmitRequest(res, paramCp)

	assert.Equal(t, lfSpine.Bytes(), req.GetLfSpine())
	assert.Equal(t, cpEpoch, req.GetResultCpEpoch())
	assert.Equal(t, cpRoot.Bytes(), req.GetResultCpRoot())
	assert.Equal(t, paramCp, req.GetParamCheckpoint())
}

func TestBuildSubmitRequest_NilPointers(t *testing.T) {
	res := &dagTypes.FinalizationResult{}
	req := buildSubmitRequest(res, nil)
	assert.Empty(t, req.GetLfSpine())
	assert.Equal(t, uint64(0), req.GetResultCpEpoch())
	assert.Empty(t, req.GetResultCpRoot())
	assert.Nil(t, req.GetParamCheckpoint())
}

// ---- processHead wiring test ----

func TestProcessHead_EndToEnd(t *testing.T) {
	spine1 := common.HexToHash("0x1111")
	base := common.HexToHash("0x2222")
	cpRoot := common.HexToHash("0x3333")
	lfSpine := common.HexToHash("0x4444")
	cpEpoch := uint64(5)

	paramsResp := &coordpb.FinalizationParamsResponse{
		Spines:    [][]byte{spine1.Bytes()},
		BaseSpine: base.Bytes(),
		Checkpoint: &coordpb.FinalizationCheckpoint{
			Epoch: cpEpoch,
			Root:  cpRoot.Bytes(),
		},
		SyncMode: uint32(dagTypes.NoSync),
	}

	finRes := &dagTypes.FinalizationResult{
		LFSpine: &lfSpine,
		CpEpoch: &cpEpoch,
		CpRoot:  &cpRoot,
	}

	fp := protoToFinalizationParams(paramsResp)
	require.NotNil(t, fp.Checkpoint)
	assert.Equal(t, cpRoot, fp.Checkpoint.Root)

	req := buildSubmitRequest(finRes, paramsResp.GetCheckpoint())
	assert.Equal(t, lfSpine.Bytes(), req.GetLfSpine())
	assert.Equal(t, cpEpoch, req.GetResultCpEpoch())
	assert.Equal(t, cpRoot.Bytes(), req.GetResultCpRoot())
}
