package rawoptional

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OptionalTime struct {
	Value    time.Time
	HasValue bool
}

func (v *OptionalTime) ToProto() *timestamppb.Timestamp {
	if v.HasValue {
		return timestamppb.New(v.Value)
	}
	return nil
}

type OptionalDuration struct {
	Value    time.Duration
	HasValue bool
}

func (v *OptionalDuration) ToProto() *durationpb.Duration {
	if v.HasValue {
		return durationpb.New(v.Value)
	}
	return nil
}
