// Code generated by protoc-gen-go. DO NOT EDIT.
// source: ydb_discovery_v1.proto

package Ydb_Discovery_V1

import (
	_ "github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Discovery"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

func init() { proto.RegisterFile("ydb_discovery_v1.proto", fileDescriptor_52888d5f10ffdaef) }

var fileDescriptor_52888d5f10ffdaef = []byte{
	// 171 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0xab, 0x4c, 0x49, 0x8a,
	0x4f, 0xc9, 0x2c, 0x4e, 0xce, 0x2f, 0x4b, 0x2d, 0xaa, 0x8c, 0x2f, 0x33, 0xd4, 0x2b, 0x28, 0xca,
	0x2f, 0xc9, 0x17, 0x12, 0x88, 0x4c, 0x49, 0xd2, 0x73, 0x81, 0x89, 0xeb, 0x85, 0x19, 0x4a, 0xe9,
	0x64, 0x67, 0x66, 0x67, 0xe6, 0x16, 0xe9, 0x17, 0x94, 0x26, 0xe5, 0x64, 0x26, 0xeb, 0x27, 0x16,
	0x64, 0xea, 0x83, 0x95, 0x16, 0xeb, 0xa3, 0x18, 0x01, 0xd1, 0x6f, 0x94, 0xc7, 0x25, 0x00, 0xd7,
	0x1d, 0x9c, 0x5a, 0x54, 0x96, 0x99, 0x9c, 0x2a, 0x14, 0xc5, 0xc5, 0xeb, 0x93, 0x59, 0x5c, 0xe2,
	0x9a, 0x97, 0x52, 0x90, 0x9f, 0x99, 0x57, 0x52, 0x2c, 0xa4, 0xac, 0x87, 0x6a, 0x0b, 0x8a, 0x6c,
	0x50, 0x6a, 0x61, 0x69, 0x6a, 0x71, 0x89, 0x94, 0x0a, 0x7e, 0x45, 0xc5, 0x05, 0xf9, 0x79, 0xc5,
	0xa9, 0x4e, 0xb2, 0x5c, 0xd2, 0xc9, 0xf9, 0xb9, 0x7a, 0x95, 0x89, 0x79, 0x29, 0xa9, 0x15, 0x7a,
	0x95, 0x29, 0x49, 0x7a, 0x08, 0x17, 0x95, 0x19, 0x26, 0xb1, 0x81, 0x5d, 0x65, 0x0c, 0x08, 0x00,
	0x00, 0xff, 0xff, 0x35, 0x87, 0x34, 0x40, 0xef, 0x00, 0x00, 0x00,
}

const (
	ListEndpoints = "/Ydb.Discovery.V1.DiscoveryService/ListEndpoints"
)