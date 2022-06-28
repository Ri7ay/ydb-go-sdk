package rawydb

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

type Operation struct {
	ID     string
	Ready  bool
	Status StatusCode
	Issues Issues
}

func (o *Operation) FromProto(proto *Ydb_Operations.Operation) error {
	o.ID = proto.GetId()
	o.Ready = proto.GetReady()
	if err := o.Status.FromProto(proto.GetStatus()); err != nil {
		return err
	}
	if err := o.Issues.FromProto(proto.Issues); err != nil {
		return err
	}
	return nil
}
