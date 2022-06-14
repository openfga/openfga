// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        (unknown)
// source: dispatch/v1/dispatch.proto

package dispatchv1

import (
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	v1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type DispatchCheckRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata       *ResolverMeta    `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	WrappedRequest *v1.CheckRequest `protobuf:"bytes,2,opt,name=wrapped_request,json=wrappedRequest,proto3" json:"wrapped_request,omitempty"`
}

func (x *DispatchCheckRequest) Reset() {
	*x = DispatchCheckRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DispatchCheckRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DispatchCheckRequest) ProtoMessage() {}

func (x *DispatchCheckRequest) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DispatchCheckRequest.ProtoReflect.Descriptor instead.
func (*DispatchCheckRequest) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{0}
}

func (x *DispatchCheckRequest) GetMetadata() *ResolverMeta {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *DispatchCheckRequest) GetWrappedRequest() *v1.CheckRequest {
	if x != nil {
		return x.WrappedRequest
	}
	return nil
}

type DispatchCheckResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata        *ResponseMeta     `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	WrappedResponse *v1.CheckResponse `protobuf:"bytes,2,opt,name=wrapped_response,json=wrappedResponse,proto3" json:"wrapped_response,omitempty"`
}

func (x *DispatchCheckResponse) Reset() {
	*x = DispatchCheckResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DispatchCheckResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DispatchCheckResponse) ProtoMessage() {}

func (x *DispatchCheckResponse) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DispatchCheckResponse.ProtoReflect.Descriptor instead.
func (*DispatchCheckResponse) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{1}
}

func (x *DispatchCheckResponse) GetMetadata() *ResponseMeta {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *DispatchCheckResponse) GetWrappedResponse() *v1.CheckResponse {
	if x != nil {
		return x.WrappedResponse
	}
	return nil
}

type DispatchExpandRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata       *ResolverMeta     `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	WrappedRequest *v1.ExpandRequest `protobuf:"bytes,2,opt,name=wrapped_request,json=wrappedRequest,proto3" json:"wrapped_request,omitempty"`
}

func (x *DispatchExpandRequest) Reset() {
	*x = DispatchExpandRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DispatchExpandRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DispatchExpandRequest) ProtoMessage() {}

func (x *DispatchExpandRequest) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DispatchExpandRequest.ProtoReflect.Descriptor instead.
func (*DispatchExpandRequest) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{2}
}

func (x *DispatchExpandRequest) GetMetadata() *ResolverMeta {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *DispatchExpandRequest) GetWrappedRequest() *v1.ExpandRequest {
	if x != nil {
		return x.WrappedRequest
	}
	return nil
}

type DispatchExpandResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Metadata        *ResponseMeta      `protobuf:"bytes,1,opt,name=metadata,proto3" json:"metadata,omitempty"`
	WrappedResponse *v1.ExpandResponse `protobuf:"bytes,2,opt,name=wrapped_response,json=wrappedResponse,proto3" json:"wrapped_response,omitempty"`
}

func (x *DispatchExpandResponse) Reset() {
	*x = DispatchExpandResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DispatchExpandResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DispatchExpandResponse) ProtoMessage() {}

func (x *DispatchExpandResponse) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DispatchExpandResponse.ProtoReflect.Descriptor instead.
func (*DispatchExpandResponse) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{3}
}

func (x *DispatchExpandResponse) GetMetadata() *ResponseMeta {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *DispatchExpandResponse) GetWrappedResponse() *v1.ExpandResponse {
	if x != nil {
		return x.WrappedResponse
	}
	return nil
}

type ResolverMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DepthRemaining uint32 `protobuf:"varint,1,opt,name=depth_remaining,json=depthRemaining,proto3" json:"depth_remaining,omitempty"`
}

func (x *ResolverMeta) Reset() {
	*x = ResolverMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResolverMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResolverMeta) ProtoMessage() {}

func (x *ResolverMeta) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResolverMeta.ProtoReflect.Descriptor instead.
func (*ResolverMeta) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{4}
}

func (x *ResolverMeta) GetDepthRemaining() uint32 {
	if x != nil {
		return x.DepthRemaining
	}
	return 0
}

type ResponseMeta struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DispatchCount       uint32 `protobuf:"varint,1,opt,name=dispatch_count,json=dispatchCount,proto3" json:"dispatch_count,omitempty"`
	DepthRequired       uint32 `protobuf:"varint,2,opt,name=depth_required,json=depthRequired,proto3" json:"depth_required,omitempty"`
	CachedDispatchCount uint32 `protobuf:"varint,3,opt,name=cached_dispatch_count,json=cachedDispatchCount,proto3" json:"cached_dispatch_count,omitempty"`
}

func (x *ResponseMeta) Reset() {
	*x = ResponseMeta{}
	if protoimpl.UnsafeEnabled {
		mi := &file_dispatch_v1_dispatch_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ResponseMeta) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ResponseMeta) ProtoMessage() {}

func (x *ResponseMeta) ProtoReflect() protoreflect.Message {
	mi := &file_dispatch_v1_dispatch_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ResponseMeta.ProtoReflect.Descriptor instead.
func (*ResponseMeta) Descriptor() ([]byte, []int) {
	return file_dispatch_v1_dispatch_proto_rawDescGZIP(), []int{5}
}

func (x *ResponseMeta) GetDispatchCount() uint32 {
	if x != nil {
		return x.DispatchCount
	}
	return 0
}

func (x *ResponseMeta) GetDepthRequired() uint32 {
	if x != nil {
		return x.DepthRequired
	}
	return 0
}

func (x *ResponseMeta) GetCachedDispatchCount() uint32 {
	if x != nil {
		return x.CachedDispatchCount
	}
	return 0
}

var File_dispatch_v1_dispatch_proto protoreflect.FileDescriptor

var file_dispatch_v1_dispatch_proto_rawDesc = []byte{
	0x0a, 0x1a, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2f, 0x76, 0x31, 0x2f, 0x64, 0x69,
	0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x64, 0x69,
	0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x1a, 0x17, 0x76, 0x61, 0x6c, 0x69, 0x64,
	0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x20, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x2f, 0x76, 0x31, 0x2f, 0x6f,
	0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x9a, 0x01, 0x0a, 0x14, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63,
	0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3f, 0x0a,
	0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x19, 0x2e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65,
	0x73, 0x6f, 0x6c, 0x76, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61, 0x42, 0x08, 0xfa, 0x42, 0x05, 0x8a,
	0x01, 0x02, 0x10, 0x01, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x41,
	0x0a, 0x0f, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x5f, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x67,
	0x61, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x52, 0x0e, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x22, 0x94, 0x01, 0x0a, 0x15, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x43, 0x68,
	0x65, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x35, 0x0a, 0x08, 0x6d,
	0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x44, 0x0a, 0x10, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x5f, 0x72, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6f,
	0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x52, 0x0f, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x9c, 0x01, 0x0a, 0x15, 0x44, 0x69, 0x73,
	0x70, 0x61, 0x74, 0x63, 0x68, 0x45, 0x78, 0x70, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x3f, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e,
	0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61, 0x42,
	0x08, 0xfa, 0x42, 0x05, 0x8a, 0x01, 0x02, 0x10, 0x01, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64,
	0x61, 0x74, 0x61, 0x12, 0x42, 0x0a, 0x0f, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x5f, 0x72,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x6f,
	0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x78, 0x70, 0x61, 0x6e, 0x64,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x52, 0x0e, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x96, 0x01, 0x0a, 0x16, 0x44, 0x69, 0x73, 0x70,
	0x61, 0x74, 0x63, 0x68, 0x45, 0x78, 0x70, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x35, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e,
	0x76, 0x31, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x52,
	0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x12, 0x45, 0x0a, 0x10, 0x77, 0x72, 0x61,
	0x70, 0x70, 0x65, 0x64, 0x5f, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x2e, 0x76, 0x31,
	0x2e, 0x45, 0x78, 0x70, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x52,
	0x0f, 0x77, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x22, 0x40, 0x0a, 0x0c, 0x52, 0x65, 0x73, 0x6f, 0x6c, 0x76, 0x65, 0x72, 0x4d, 0x65, 0x74, 0x61,
	0x12, 0x30, 0x0a, 0x0f, 0x64, 0x65, 0x70, 0x74, 0x68, 0x5f, 0x72, 0x65, 0x6d, 0x61, 0x69, 0x6e,
	0x69, 0x6e, 0x67, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x07, 0xfa, 0x42, 0x04, 0x2a, 0x02,
	0x20, 0x00, 0x52, 0x0e, 0x64, 0x65, 0x70, 0x74, 0x68, 0x52, 0x65, 0x6d, 0x61, 0x69, 0x6e, 0x69,
	0x6e, 0x67, 0x22, 0x90, 0x01, 0x0a, 0x0c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x4d,
	0x65, 0x74, 0x61, 0x12, 0x25, 0x0a, 0x0e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x5f,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x64, 0x69, 0x73,
	0x70, 0x61, 0x74, 0x63, 0x68, 0x43, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x25, 0x0a, 0x0e, 0x64, 0x65,
	0x70, 0x74, 0x68, 0x5f, 0x72, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0d, 0x52, 0x0d, 0x64, 0x65, 0x70, 0x74, 0x68, 0x52, 0x65, 0x71, 0x75, 0x69, 0x72, 0x65,
	0x64, 0x12, 0x32, 0x0a, 0x15, 0x63, 0x61, 0x63, 0x68, 0x65, 0x64, 0x5f, 0x64, 0x69, 0x73, 0x70,
	0x61, 0x74, 0x63, 0x68, 0x5f, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d,
	0x52, 0x13, 0x63, 0x61, 0x63, 0x68, 0x65, 0x64, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68,
	0x43, 0x6f, 0x75, 0x6e, 0x74, 0x32, 0xc8, 0x01, 0x0a, 0x0f, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74,
	0x63, 0x68, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x58, 0x0a, 0x0d, 0x44, 0x69, 0x73,
	0x70, 0x61, 0x74, 0x63, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x12, 0x21, 0x2e, 0x64, 0x69, 0x73,
	0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63,
	0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x22, 0x2e,
	0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x69, 0x73, 0x70,
	0x61, 0x74, 0x63, 0x68, 0x43, 0x68, 0x65, 0x63, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x22, 0x00, 0x12, 0x5b, 0x0a, 0x0e, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x45,
	0x78, 0x70, 0x61, 0x6e, 0x64, 0x12, 0x22, 0x2e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68,
	0x2e, 0x76, 0x31, 0x2e, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x45, 0x78, 0x70, 0x61,
	0x6e, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x23, 0x2e, 0x64, 0x69, 0x73, 0x70,
	0x61, 0x74, 0x63, 0x68, 0x2e, 0x76, 0x31, 0x2e, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68,
	0x45, 0x78, 0x70, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00,
	0x42, 0xaa, 0x01, 0x0a, 0x0f, 0x63, 0x6f, 0x6d, 0x2e, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63,
	0x68, 0x2e, 0x76, 0x31, 0x42, 0x0d, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x3b, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x67, 0x61, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x67,
	0x61, 0x2f, 0x70, 0x6b, 0x67, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x64, 0x69, 0x73, 0x70,
	0x61, 0x74, 0x63, 0x68, 0x2f, 0x76, 0x31, 0x3b, 0x64, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68,
	0x76, 0x31, 0xa2, 0x02, 0x03, 0x44, 0x58, 0x58, 0xaa, 0x02, 0x0b, 0x44, 0x69, 0x73, 0x70, 0x61,
	0x74, 0x63, 0x68, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x0b, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63,
	0x68, 0x5c, 0x56, 0x31, 0xe2, 0x02, 0x17, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x5c,
	0x56, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea, 0x02,
	0x0c, 0x44, 0x69, 0x73, 0x70, 0x61, 0x74, 0x63, 0x68, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_dispatch_v1_dispatch_proto_rawDescOnce sync.Once
	file_dispatch_v1_dispatch_proto_rawDescData = file_dispatch_v1_dispatch_proto_rawDesc
)

func file_dispatch_v1_dispatch_proto_rawDescGZIP() []byte {
	file_dispatch_v1_dispatch_proto_rawDescOnce.Do(func() {
		file_dispatch_v1_dispatch_proto_rawDescData = protoimpl.X.CompressGZIP(file_dispatch_v1_dispatch_proto_rawDescData)
	})
	return file_dispatch_v1_dispatch_proto_rawDescData
}

var file_dispatch_v1_dispatch_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_dispatch_v1_dispatch_proto_goTypes = []interface{}{
	(*DispatchCheckRequest)(nil),   // 0: dispatch.v1.DispatchCheckRequest
	(*DispatchCheckResponse)(nil),  // 1: dispatch.v1.DispatchCheckResponse
	(*DispatchExpandRequest)(nil),  // 2: dispatch.v1.DispatchExpandRequest
	(*DispatchExpandResponse)(nil), // 3: dispatch.v1.DispatchExpandResponse
	(*ResolverMeta)(nil),           // 4: dispatch.v1.ResolverMeta
	(*ResponseMeta)(nil),           // 5: dispatch.v1.ResponseMeta
	(*v1.CheckRequest)(nil),        // 6: openfga.v1.CheckRequest
	(*v1.CheckResponse)(nil),       // 7: openfga.v1.CheckResponse
	(*v1.ExpandRequest)(nil),       // 8: openfga.v1.ExpandRequest
	(*v1.ExpandResponse)(nil),      // 9: openfga.v1.ExpandResponse
}
var file_dispatch_v1_dispatch_proto_depIdxs = []int32{
	4,  // 0: dispatch.v1.DispatchCheckRequest.metadata:type_name -> dispatch.v1.ResolverMeta
	6,  // 1: dispatch.v1.DispatchCheckRequest.wrapped_request:type_name -> openfga.v1.CheckRequest
	5,  // 2: dispatch.v1.DispatchCheckResponse.metadata:type_name -> dispatch.v1.ResponseMeta
	7,  // 3: dispatch.v1.DispatchCheckResponse.wrapped_response:type_name -> openfga.v1.CheckResponse
	4,  // 4: dispatch.v1.DispatchExpandRequest.metadata:type_name -> dispatch.v1.ResolverMeta
	8,  // 5: dispatch.v1.DispatchExpandRequest.wrapped_request:type_name -> openfga.v1.ExpandRequest
	5,  // 6: dispatch.v1.DispatchExpandResponse.metadata:type_name -> dispatch.v1.ResponseMeta
	9,  // 7: dispatch.v1.DispatchExpandResponse.wrapped_response:type_name -> openfga.v1.ExpandResponse
	0,  // 8: dispatch.v1.DispatchService.DispatchCheck:input_type -> dispatch.v1.DispatchCheckRequest
	2,  // 9: dispatch.v1.DispatchService.DispatchExpand:input_type -> dispatch.v1.DispatchExpandRequest
	1,  // 10: dispatch.v1.DispatchService.DispatchCheck:output_type -> dispatch.v1.DispatchCheckResponse
	3,  // 11: dispatch.v1.DispatchService.DispatchExpand:output_type -> dispatch.v1.DispatchExpandResponse
	10, // [10:12] is the sub-list for method output_type
	8,  // [8:10] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
}

func init() { file_dispatch_v1_dispatch_proto_init() }
func file_dispatch_v1_dispatch_proto_init() {
	if File_dispatch_v1_dispatch_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_dispatch_v1_dispatch_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DispatchCheckRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dispatch_v1_dispatch_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DispatchCheckResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dispatch_v1_dispatch_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DispatchExpandRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dispatch_v1_dispatch_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DispatchExpandResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dispatch_v1_dispatch_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResolverMeta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_dispatch_v1_dispatch_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ResponseMeta); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_dispatch_v1_dispatch_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_dispatch_v1_dispatch_proto_goTypes,
		DependencyIndexes: file_dispatch_v1_dispatch_proto_depIdxs,
		MessageInfos:      file_dispatch_v1_dispatch_proto_msgTypes,
	}.Build()
	File_dispatch_v1_dispatch_proto = out.File
	file_dispatch_v1_dispatch_proto_rawDesc = nil
	file_dispatch_v1_dispatch_proto_goTypes = nil
	file_dispatch_v1_dispatch_proto_depIdxs = nil
}
