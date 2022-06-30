package options

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

type CreateTopicOption func(request *rawtopic.CreateTopicRequest)

func CreateTopicWithConsumer(consumer Consumer) CreateTopicOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.Consumers = append(request.Consumers, consumer)
	}
}
