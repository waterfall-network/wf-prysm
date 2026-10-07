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

// Hand-written gRPC service bindings for coordinator_internal.proto.
// Source: proto/prysm/v1alpha1/coordinator_internal.proto

package eth

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// CoordinatorInternalClient is the client API for CoordinatorInternal service.
type CoordinatorInternalClient interface {
	// StreamNewHeads streams a notification for every new canonical head block.
	StreamNewHeads(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (CoordinatorInternal_StreamNewHeadsClient, error)
	// GetFinalizationParams returns the beacon-state-derived finalization params.
	GetFinalizationParams(ctx context.Context, in *GetFinalizationParamsRequest, opts ...grpc.CallOption) (*FinalizationParamsResponse, error)
	// SubmitFinalizationResult sends the gwat finalization outcome back.
	SubmitFinalizationResult(ctx context.Context, in *SubmitFinalizationResultRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type coordinatorInternalClient struct {
	cc grpc.ClientConnInterface
}

func NewCoordinatorInternalClient(cc grpc.ClientConnInterface) CoordinatorInternalClient {
	return &coordinatorInternalClient{cc}
}

func (c *coordinatorInternalClient) StreamNewHeads(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (CoordinatorInternal_StreamNewHeadsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_CoordinatorInternal_serviceDesc.Streams[0], "/ethereum.eth.v1alpha1.CoordinatorInternal/StreamNewHeads", opts...)
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

// CoordinatorInternal_StreamNewHeadsClient is the streaming client interface for StreamNewHeads.
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

// CoordinatorInternalServer is the server API for CoordinatorInternal service.
type CoordinatorInternalServer interface {
	// StreamNewHeads streams a notification for every new canonical head block.
	StreamNewHeads(*emptypb.Empty, CoordinatorInternal_StreamNewHeadsServer) error
	// GetFinalizationParams returns the beacon-state-derived finalization params.
	GetFinalizationParams(context.Context, *GetFinalizationParamsRequest) (*FinalizationParamsResponse, error)
	// SubmitFinalizationResult sends the gwat finalization outcome back.
	SubmitFinalizationResult(context.Context, *SubmitFinalizationResultRequest) (*emptypb.Empty, error)
}

// UnimplementedCoordinatorInternalServer can be embedded to have forward compatible implementations.
type UnimplementedCoordinatorInternalServer struct{}

func (*UnimplementedCoordinatorInternalServer) StreamNewHeads(*emptypb.Empty, CoordinatorInternal_StreamNewHeadsServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamNewHeads not implemented")
}

func (*UnimplementedCoordinatorInternalServer) GetFinalizationParams(context.Context, *GetFinalizationParamsRequest) (*FinalizationParamsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetFinalizationParams not implemented")
}

func (*UnimplementedCoordinatorInternalServer) SubmitFinalizationResult(context.Context, *SubmitFinalizationResultRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SubmitFinalizationResult not implemented")
}

// RegisterCoordinatorInternalServer registers CoordinatorInternalServer on grpc.Server.
func RegisterCoordinatorInternalServer(s *grpc.Server, srv CoordinatorInternalServer) {
	s.RegisterService(&_CoordinatorInternal_serviceDesc, srv)
}

func _CoordinatorInternal_StreamNewHeads_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(CoordinatorInternalServer).StreamNewHeads(m, &coordinatorInternalStreamNewHeadsServer{stream})
}

// CoordinatorInternal_StreamNewHeadsServer is the streaming server interface for StreamNewHeads.
type CoordinatorInternal_StreamNewHeadsServer interface {
	Send(*NewHeadEvent) error
	grpc.ServerStream
}

type coordinatorInternalStreamNewHeadsServer struct {
	grpc.ServerStream
}

func (x *coordinatorInternalStreamNewHeadsServer) Send(m *NewHeadEvent) error {
	return x.ServerStream.SendMsg(m)
}

func _CoordinatorInternal_GetFinalizationParams_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetFinalizationParamsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorInternalServer).GetFinalizationParams(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/ethereum.eth.v1alpha1.CoordinatorInternal/GetFinalizationParams",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorInternalServer).GetFinalizationParams(ctx, req.(*GetFinalizationParamsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _CoordinatorInternal_SubmitFinalizationResult_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SubmitFinalizationResultRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(CoordinatorInternalServer).SubmitFinalizationResult(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/ethereum.eth.v1alpha1.CoordinatorInternal/SubmitFinalizationResult",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(CoordinatorInternalServer).SubmitFinalizationResult(ctx, req.(*SubmitFinalizationResultRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _CoordinatorInternal_serviceDesc = grpc.ServiceDesc{
	ServiceName: "ethereum.eth.v1alpha1.CoordinatorInternal",
	HandlerType: (*CoordinatorInternalServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFinalizationParams",
			Handler:    _CoordinatorInternal_GetFinalizationParams_Handler,
		},
		{
			MethodName: "SubmitFinalizationResult",
			Handler:    _CoordinatorInternal_SubmitFinalizationResult_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamNewHeads",
			Handler:       _CoordinatorInternal_StreamNewHeads_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "proto/prysm/v1alpha1/coordinator_internal.proto",
}
