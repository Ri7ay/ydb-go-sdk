package rawtopiccommon

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type ServerMessageMetadata struct {
	Status rawydb.StatusCode
	Issues rawydb.Issues
}

func (m *ServerMessageMetadata) MetaFromProto(p *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage) error {
	if err := m.Status.FromProto(p.Status); err != nil {
		return err
	}

	return m.Issues.FromProto(p.Issues)
}

func (m *ServerMessageMetadata) StatusData() ServerMessageMetadata {
	return *m
}

func (m *ServerMessageMetadata) SetStatus(status rawydb.StatusCode) {
	m.Status = status
}
