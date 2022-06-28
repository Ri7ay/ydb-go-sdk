package topicstream

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

type AlterTopicOption func(req *rawtopic.AlterTopicRequest)

func AddConsumer(consumer Consumer) AlterTopicOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.AddConsumer = append(req.AddConsumer, consumer)
	}
}

type Consumer = rawtopic.Consumer
