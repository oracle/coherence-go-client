//
// Copyright (c) 2020, 2024, Oracle and/or its affiliates.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
// https://oss.oracle.com/licenses/upl.

// -----------------------------------------------------------------
// Common messages used by various Coherence services.
// -----------------------------------------------------------------

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.33.0
// 	protoc        v3.19.2
// source: common_messages_v1.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	_ "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// An error message
type ErrorMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The test of the error message
	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
	// An optional Exception serialized using the client's serializer
	Error []byte `protobuf:"bytes,2,opt,name=error,proto3,oneof" json:"error,omitempty"`
}

func (x *ErrorMessage) Reset() {
	*x = ErrorMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ErrorMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ErrorMessage) ProtoMessage() {}

func (x *ErrorMessage) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ErrorMessage.ProtoReflect.Descriptor instead.
func (*ErrorMessage) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{0}
}

func (x *ErrorMessage) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

func (x *ErrorMessage) GetError() []byte {
	if x != nil {
		return x.Error
	}
	return nil
}

// A message to indicate completion of a request response.
type Complete struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Complete) Reset() {
	*x = Complete{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Complete) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Complete) ProtoMessage() {}

func (x *Complete) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Complete.ProtoReflect.Descriptor instead.
func (*Complete) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{1}
}

// A heart beat message.
type HeartbeatMessage struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The UUID of the client
	Uuid []byte `protobuf:"bytes,1,opt,name=uuid,proto3,oneof" json:"uuid,omitempty"`
	// True to send a heartbeat response
	Ack bool `protobuf:"varint,2,opt,name=ack,proto3" json:"ack,omitempty"`
}

func (x *HeartbeatMessage) Reset() {
	*x = HeartbeatMessage{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HeartbeatMessage) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartbeatMessage) ProtoMessage() {}

func (x *HeartbeatMessage) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartbeatMessage.ProtoReflect.Descriptor instead.
func (*HeartbeatMessage) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{2}
}

func (x *HeartbeatMessage) GetUuid() []byte {
	if x != nil {
		return x.Uuid
	}
	return nil
}

func (x *HeartbeatMessage) GetAck() bool {
	if x != nil {
		return x.Ack
	}
	return false
}

// An optional value.
type OptionalValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// A flag indicating whether the value is present.
	Present bool `protobuf:"varint,1,opt,name=present,proto3" json:"present,omitempty"`
	// The serialized value.
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *OptionalValue) Reset() {
	*x = OptionalValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OptionalValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OptionalValue) ProtoMessage() {}

func (x *OptionalValue) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OptionalValue.ProtoReflect.Descriptor instead.
func (*OptionalValue) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{3}
}

func (x *OptionalValue) GetPresent() bool {
	if x != nil {
		return x.Present
	}
	return false
}

func (x *OptionalValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

// A message that contains a collection of serialized binary values.
type CollectionOfBytesValues struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The serialized values
	Values [][]byte `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *CollectionOfBytesValues) Reset() {
	*x = CollectionOfBytesValues{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CollectionOfBytesValues) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CollectionOfBytesValues) ProtoMessage() {}

func (x *CollectionOfBytesValues) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CollectionOfBytesValues.ProtoReflect.Descriptor instead.
func (*CollectionOfBytesValues) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{4}
}

func (x *CollectionOfBytesValues) GetValues() [][]byte {
	if x != nil {
		return x.Values
	}
	return nil
}

// A message containing a serialized key and value.
type BinaryKeyAndValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// The serialized binary key.
	Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	// The serialized binary value.
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *BinaryKeyAndValue) Reset() {
	*x = BinaryKeyAndValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_common_messages_v1_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BinaryKeyAndValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BinaryKeyAndValue) ProtoMessage() {}

func (x *BinaryKeyAndValue) ProtoReflect() protoreflect.Message {
	mi := &file_common_messages_v1_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BinaryKeyAndValue.ProtoReflect.Descriptor instead.
func (*BinaryKeyAndValue) Descriptor() ([]byte, []int) {
	return file_common_messages_v1_proto_rawDescGZIP(), []int{5}
}

func (x *BinaryKeyAndValue) GetKey() []byte {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *BinaryKeyAndValue) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

var File_common_messages_v1_proto protoreflect.FileDescriptor

var file_common_messages_v1_proto_rawDesc = []byte{
	0x0a, 0x18, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x73, 0x5f, 0x76, 0x31, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x13, 0x63, 0x6f, 0x68, 0x65,
	0x72, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x1a,
	0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x4d, 0x0a, 0x0c, 0x45, 0x72,
	0x72, 0x6f, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65,
	0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x12, 0x19, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x88, 0x01, 0x01, 0x42,
	0x08, 0x0a, 0x06, 0x5f, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x0a, 0x0a, 0x08, 0x43, 0x6f, 0x6d,
	0x70, 0x6c, 0x65, 0x74, 0x65, 0x22, 0x46, 0x0a, 0x10, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65,
	0x61, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12, 0x17, 0x0a, 0x04, 0x75, 0x75, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x04, 0x75, 0x75, 0x69, 0x64, 0x88,
	0x01, 0x01, 0x12, 0x10, 0x0a, 0x03, 0x61, 0x63, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x03, 0x61, 0x63, 0x6b, 0x42, 0x07, 0x0a, 0x05, 0x5f, 0x75, 0x75, 0x69, 0x64, 0x22, 0x3f, 0x0a,
	0x0d, 0x4f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x18,
	0x0a, 0x07, 0x70, 0x72, 0x65, 0x73, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52,
	0x07, 0x70, 0x72, 0x65, 0x73, 0x65, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x31,
	0x0a, 0x17, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x4f, 0x66, 0x42, 0x79,
	0x74, 0x65, 0x73, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0c, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x22, 0x3b, 0x0a, 0x11, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x4b, 0x65, 0x79, 0x41, 0x6e,
	0x64, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0c, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x60,
	0x0a, 0x2c, 0x63, 0x6f, 0x6d, 0x2e, 0x6f, 0x72, 0x61, 0x63, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x68,
	0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x6d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x50, 0x01,
	0x5a, 0x2e, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x72, 0x61,
	0x63, 0x6c, 0x65, 0x2f, 0x63, 0x6f, 0x68, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x2d, 0x67, 0x6f,
	0x2d, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x76, 0x31,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_common_messages_v1_proto_rawDescOnce sync.Once
	file_common_messages_v1_proto_rawDescData = file_common_messages_v1_proto_rawDesc
)

func file_common_messages_v1_proto_rawDescGZIP() []byte {
	file_common_messages_v1_proto_rawDescOnce.Do(func() {
		file_common_messages_v1_proto_rawDescData = protoimpl.X.CompressGZIP(file_common_messages_v1_proto_rawDescData)
	})
	return file_common_messages_v1_proto_rawDescData
}

var file_common_messages_v1_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_common_messages_v1_proto_goTypes = []interface{}{
	(*ErrorMessage)(nil),            // 0: coherence.common.v1.ErrorMessage
	(*Complete)(nil),                // 1: coherence.common.v1.Complete
	(*HeartbeatMessage)(nil),        // 2: coherence.common.v1.HeartbeatMessage
	(*OptionalValue)(nil),           // 3: coherence.common.v1.OptionalValue
	(*CollectionOfBytesValues)(nil), // 4: coherence.common.v1.CollectionOfBytesValues
	(*BinaryKeyAndValue)(nil),       // 5: coherence.common.v1.BinaryKeyAndValue
}
var file_common_messages_v1_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_common_messages_v1_proto_init() }
func file_common_messages_v1_proto_init() {
	if File_common_messages_v1_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_common_messages_v1_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ErrorMessage); i {
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
		file_common_messages_v1_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Complete); i {
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
		file_common_messages_v1_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HeartbeatMessage); i {
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
		file_common_messages_v1_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OptionalValue); i {
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
		file_common_messages_v1_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CollectionOfBytesValues); i {
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
		file_common_messages_v1_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BinaryKeyAndValue); i {
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
	file_common_messages_v1_proto_msgTypes[0].OneofWrappers = []interface{}{}
	file_common_messages_v1_proto_msgTypes[2].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_common_messages_v1_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_common_messages_v1_proto_goTypes,
		DependencyIndexes: file_common_messages_v1_proto_depIdxs,
		MessageInfos:      file_common_messages_v1_proto_msgTypes,
	}.Build()
	File_common_messages_v1_proto = out.File
	file_common_messages_v1_proto_rawDesc = nil
	file_common_messages_v1_proto_goTypes = nil
	file_common_messages_v1_proto_depIdxs = nil
}
