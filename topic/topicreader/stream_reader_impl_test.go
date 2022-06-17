package topicreader_test

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func BenchmarkMassCommit(b *testing.B) {
	source := make([]topicreader.Message, 10000)
	for i := range source {
		source[i].Offset.FromInt64(int64(i))
		source[i].ToOffset.FromInt64(int64(i + 1))
	}

	first := source[0]
	res := source[:1]
	last := &source[0]
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		res = source[:1]
		last = &source[0]
		*last = first

		for i := 1; i < len(source); i++ {
			if last.ToOffset == source[i].Offset {
				last.ToOffset = source[i].ToOffset
			} else {
				res = append(res, source[i])
				last = &source[len(source)-1]
			}
		}
	}

	if len(res) != 1 {
		b.Error(len(res))
	}
	if res[0].ToOffset != source[len(source)-1].ToOffset {
		b.Error()
	}
}
