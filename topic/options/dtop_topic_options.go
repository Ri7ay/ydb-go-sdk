package options

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

type DropTopicOption func(request *rawtopic.DropTopicRequest)
