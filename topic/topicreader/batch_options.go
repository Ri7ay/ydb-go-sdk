package topicreader

// ReadBatchOption для различных пожеланий к батчу вроде WithMaxMessages(int)
type ReadBatchOption func(options *readMessageBatchOptions)

func WithBatchMaxCount(count int) ReadBatchOption {
	return func(options *readMessageBatchOptions) {
		options.MaxCount = count
	}
}

func WithBatchMinCount(count int) ReadBatchOption {
	return func(options *readMessageBatchOptions) {
		options.MinCount = count
	}
}

func readExplicitMessagesCount(count int) ReadBatchOption {
	return func(options *readMessageBatchOptions) {
		options.MinCount = count
		options.MaxCount = count
	}
}
