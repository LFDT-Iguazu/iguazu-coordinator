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

// Hand-written gRPC client bindings for coordinator_internal.proto.
// Source: proto/prysm/v1alpha1/coordinator_internal.proto

package coordpb

import (
	context "context"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var _ context.Context
var _ grpc.ClientConnInterface

const _ = grpc.SupportPackageIsVersion6

// CoordinatorInternalClient is the client API for CoordinatorInternal service.
type CoordinatorInternalClient interface {
	StreamNewHeads(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (CoordinatorInternal_StreamNewHeadsClient, error)
	GetFinalizationParams(ctx context.Context, in *GetFinalizationParamsRequest, opts ...grpc.CallOption) (*FinalizationParamsResponse, error)
	SubmitFinalizationResult(ctx context.Context, in *SubmitFinalizationResultRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type coordinatorInternalClient struct {
	cc grpc.ClientConnInterface
}

func NewCoordinatorInternalClient(cc grpc.ClientConnInterface) CoordinatorInternalClient {
	return &coordinatorInternalClient{cc}
}

func (c *coordinatorInternalClient) StreamNewHeads(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (CoordinatorInternal_StreamNewHeadsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_CoordinatorInternal_streamDesc, "/ethereum.eth.v1alpha1.CoordinatorInternal/StreamNewHeads", opts...)
	if err != nil {
		return nil, err
	}
	x := &coordinatorInternalStreamNewHeadsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

var _CoordinatorInternal_streamDesc = grpc.StreamDesc{
	StreamName:    "StreamNewHeads",
	ServerStreams: true,
}

type CoordinatorInternal_StreamNewHeadsClient interface {
	Recv() (*NewHeadEvent, error)
	grpc.ClientStream
}

type coordinatorInternalStreamNewHeadsClient struct {
	grpc.ClientStream
}

func (x *coordinatorInternalStreamNewHeadsClient) Recv() (*NewHeadEvent, error) {
	m := new(NewHeadEvent)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *coordinatorInternalClient) GetFinalizationParams(ctx context.Context, in *GetFinalizationParamsRequest, opts ...grpc.CallOption) (*FinalizationParamsResponse, error) {
	out := new(FinalizationParamsResponse)
	err := c.cc.Invoke(ctx, "/ethereum.eth.v1alpha1.CoordinatorInternal/GetFinalizationParams", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *coordinatorInternalClient) SubmitFinalizationResult(ctx context.Context, in *SubmitFinalizationResultRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/ethereum.eth.v1alpha1.CoordinatorInternal/SubmitFinalizationResult", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
