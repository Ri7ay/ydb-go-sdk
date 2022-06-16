package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_ClusterDiscovery"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type DiscoveryClient interface {
	DiscoverForWrite(context.Context, WriteParam) (DiscoveryResult, error)
	DiscoverForRead(context.Context, ReadParam) (DiscoveryResult, error)
}

type DiscoveryFabric func(context.Context, ydb.Connection) (DiscoveryClient, error)

type Connector struct {
	ctx     context.Context
	cancel  context.CancelFunc
	options []ydb.Option

	mainConn   ydb.Connection
	discovery  DiscoveryClient
	portSuffix string

	mu          sync.Mutex
	connections map[connKey]ydb.Connection
}

func NewCluster(ctx context.Context, df DiscoveryFabric, options ...ydb.Option) (*Connector, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := ydb.New(ctx, options...)
	if err != nil {
		return nil, err
	}

	d, err := df(ctx, conn)
	if err != nil {
		return nil, err
	}

	portSuffix := ""
	if parts := strings.Split(conn.Endpoint(), ":"); len(parts) > 0 {
		portSuffix = ":" + parts[len(parts)-1]
	}

	clusterCtx, clusterCancel := context.WithCancel(context.TODO())
	return &Connector{
		ctx:         clusterCtx,
		cancel:      clusterCancel,
		options:     options,
		mainConn:    conn,
		discovery:   d,
		portSuffix:  portSuffix,
		connections: make(map[connKey]ydb.Connection),
	}, nil
}

func (c *Connector) DescribeForWrite(ctx context.Context, p WriteParam) (WriteSessionClusters, error) {
	dr, err := c.discovery.DiscoverForWrite(ctx, p)
	if err != nil || len(dr.WriteSessionsClusters) == 0 {
		return WriteSessionClusters{}, err
	}
	return dr.WriteSessionsClusters[0], nil
}

func (c *Connector) DescribeForRead(ctx context.Context, p ReadParam) (ReadSessionClusters, error) {
	dr, err := c.discovery.DiscoverForRead(ctx, p)
	if err != nil || len(dr.ReadSessionsClusters) == 0 {
		return ReadSessionClusters{}, err
	}
	return dr.ReadSessionsClusters[0], nil
}

func (c *Connector) ConnectForWrite(ctx context.Context, p WriteParam) (conn ydb.Connection, err error) {
	defer func() { err = xerrors.WithStackTrace(err, xerrors.WithSkipDepth(1)) }()

	dr, err := c.discovery.DiscoverForWrite(ctx, p)
	switch {
	case err != nil:
		return nil, err
	case len(dr.WriteSessionsClusters) == 0:
		return nil, errors.New("no write discovery results")
	}

	var info Info
	for _, cl := range dr.WriteSessionsClusters[0].Clusters {
		if !cl.Available {
			continue
		}
		info = cl
		break
	}
	if info.Endpoint == "" {
		return nil, errors.New("no available write clusters")
	}
	return c.connect(info)
}

func (c *Connector) ConnectForRead(ctx context.Context, p ReadParam) (conn ydb.Connection, err error) {
	defer func() { err = xerrors.WithStackTrace(err, xerrors.WithSkipDepth(1)) }()

	dr, err := c.discovery.DiscoverForRead(ctx, p)
	switch {
	case err != nil:
		return nil, nil
	case len(dr.ReadSessionsClusters) == 0:
		return nil, errors.New("no read discovery results")
	}

	var info Info
	for _, cl := range dr.ReadSessionsClusters[0].Clusters {
		if !cl.Available {
			continue
		}
		info = cl
		break
	}
	if info.Endpoint == "" {
		return nil, errors.New("no available read clusters")
	}
	return c.connect(info)
}

func (c *Connector) connect(info Info) (ydb.Connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := connKey{endpoint: info.Endpoint + c.portSuffix, cluster: info.Name}
	if _, found := c.connections[key]; !found {
		con, err := c.mainConn.With(
			context.TODO(), // c.ctx,
			ydb.WithDialTimeout(time.Second*5),
			ydb.WithEndpoint(key.endpoint),
			ydb.WithPersqueueCluster(key.cluster),
		)
		if err != nil {
			return nil, err
		}
		c.connections[key] = con
	}

	return c.connections[key], nil
}

type WriteParam struct {
	Stream               scheme.Path
	MessageGroupID       string
	PartitionGroup       uint
	PreferredClusterName string
}

type ReadParam struct {
	Stream        scheme.Path
	MirrorCluster string // empty means all original
}

type DiscoveryResult struct {
	WriteSessionsClusters []WriteSessionClusters
	ReadSessionsClusters  []ReadSessionClusters
	Version               int
}

type WriteSessionClusters struct {
	Clusters                      []Info
	PrimaryClusterSelectionReason SelectionReason
}

type ReadSessionClusters struct {
	Clusters []Info
}

type Info struct {
	Endpoint  string
	Name      string
	Available bool
}

type SelectionReason uint8

const (
	ReasonUnscpecified = iota
	ReasonClientPreference
	ReasonClientLocation
	ReasonConsistentDistribution
)

func (r *DiscoveryResult) From(v *Ydb_PersQueue_ClusterDiscovery.DiscoverClustersResult) {
	if v == nil {
		*r = DiscoveryResult{}
		return
	}
	*r = DiscoveryResult{
		Version: int(v.Version),
	}

	for _, wc := range v.WriteSessionsClusters {
		if wc == nil {
			continue
		}
		cl := WriteSessionClusters{
			PrimaryClusterSelectionReason: decodeSelectionReason(wc.PrimaryClusterSelectionReason),
		}
		for _, c := range wc.Clusters {
			cl.Clusters = append(cl.Clusters, Info{
				Endpoint:  c.Endpoint,
				Name:      c.Name,
				Available: c.Available,
			})
		}
		r.WriteSessionsClusters = append(r.WriteSessionsClusters, cl)
	}

	for _, rc := range v.ReadSessionsClusters {
		if rc == nil {
			continue
		}
		cl := ReadSessionClusters{}
		for _, c := range rc.Clusters {
			cl.Clusters = append(cl.Clusters, Info{
				Endpoint:  c.Endpoint,
				Name:      c.Name,
				Available: c.Available,
			})
		}
		r.ReadSessionsClusters = append(r.ReadSessionsClusters, cl)
	}
}

func decodeSelectionReason(v Ydb_PersQueue_ClusterDiscovery.WriteSessionClusters_SelectionReason) SelectionReason {
	switch v {
	case Ydb_PersQueue_ClusterDiscovery.WriteSessionClusters_SELECTION_REASON_UNSPECIFIED:
		return ReasonUnscpecified
	case Ydb_PersQueue_ClusterDiscovery.WriteSessionClusters_CLIENT_PREFERENCE:
		return ReasonClientPreference
	case Ydb_PersQueue_ClusterDiscovery.WriteSessionClusters_CLIENT_LOCATION:
		return ReasonClientLocation
	case Ydb_PersQueue_ClusterDiscovery.WriteSessionClusters_CONSISTENT_DISTRIBUTION:
		return ReasonConsistentDistribution
	default:
		panic(fmt.Sprintf("unknown selection reason %v", v))
	}
}

type connKey struct {
	endpoint string
	cluster  string
}
