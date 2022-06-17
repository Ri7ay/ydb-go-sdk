package cluster

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pqproto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_ClusterDiscovery"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/config"
)

type Connector interface {
	grpc.ClientConnInterface
	Name() string
}

type PQDiscovery struct {
	config  config.Config
	service Ydb_PersQueue_V1.ClusterDiscoveryServiceClient
}

func NewPQDiscovery(cc Connector, options []config.Option) *PQDiscovery {
	return &PQDiscovery{
		config:  config.New(options...),
		service: Ydb_PersQueue_V1.NewClusterDiscoveryServiceClient(cc),
	}
}

func (d *PQDiscovery) DiscoverForWrite(
	ctx context.Context, p cluster.WriteParam,
) (res cluster.DiscoveryResult, err error) {
	param := pqproto.WriteSessionParams{
		Topic:                string(p.Stream),
		SourceId:             []byte(p.MessageGroupID),
		PartitionGroup:       uint32(p.PartitionGroup),
		PreferredClusterName: p.PreferredClusterName,
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = d.discover(ctx, &param, nil)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true))
	return res, err
}

func (d *PQDiscovery) DiscoverForRead(
	ctx context.Context, p cluster.ReadParam,
) (res cluster.DiscoveryResult, err error) {
	param := pqproto.ReadSessionParams{
		Topic: string(p.Stream),
	}
	switch p.MirrorCluster {
	case "":
		param.ReadRule = &pqproto.ReadSessionParams_AllOriginal{}
	default:
		param.ReadRule = &pqproto.ReadSessionParams_MirrorToCluster{MirrorToCluster: p.MirrorCluster}
	}
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		res, err = d.discover(ctx, nil, &param)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true))
	return res, err
}

func (d *PQDiscovery) discover(
	ctx context.Context,
	w *pqproto.WriteSessionParams,
	r *pqproto.ReadSessionParams,
) (result cluster.DiscoveryResult, err error) {
	param := pqproto.DiscoverClustersRequest{
		OperationParams: operation.Sync(ctx, d.config),
	}
	if w != nil {
		param.WriteSessions = []*pqproto.WriteSessionParams{w}
	}
	if r != nil {
		param.ReadSessions = []*pqproto.ReadSessionParams{r}
	}
	response, err := d.service.DiscoverClusters(ctx, &param)
	if err != nil {
		return cluster.DiscoveryResult{}, err
	}

	var opResult pqproto.DiscoverClustersResult
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &opResult)
	if err != nil {
		return result, xerrors.WithStackTrace(err)
	}

	result.From(&opResult)
	return result, err
}
