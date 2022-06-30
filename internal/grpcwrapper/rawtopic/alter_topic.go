package rawtopic

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type AlterTopicRequest struct {
	OperationParams rawydb.OperationParams
	Path            string
	AddConsumer     []Consumer
}

func (req *AlterTopicRequest) ToProto() *Ydb_PersQueue_V1.AddReadRuleRequest {
	panic("not implemented")
}

type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs SupportedCodecs
	ReadFrom        rawoptional.OptionalTime
	Attributes      map[string]string
}

func (c *Consumer) MustFromProto(consumer *Ydb_Topic.Consumer) {
	c.Name = consumer.GetName()
	c.Important = consumer.GetImportant()
	c.Attributes = consumer.GetAttributes()
	c.ReadFrom.MustFromProto(consumer.GetReadFrom())
	c.SupportedCodecs.MustFromProto(consumer.SupportedCodecs)
}

func (c *Consumer) ToProto() *Ydb_Topic.Consumer {
	return &Ydb_Topic.Consumer{
		Name:            c.Name,
		Important:       c.Important,
		ReadFrom:        c.ReadFrom.ToProto(),
		SupportedCodecs: c.SupportedCodecs.ToProto(),
		Attributes:      c.Attributes,
	}
}

type AlterTopicResult struct{}

func (r *AlterTopicResult) FromProto(proto *Ydb_PersQueue_V1.AlterTopicResponse) error {
	panic("not implemented")
}
