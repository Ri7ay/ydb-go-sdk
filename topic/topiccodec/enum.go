package topiccodec

const (
	unspecifiedLabel = "Unspecified"
	unknownLabel     = "Unknown"
)

// Codec for stream data encoding settings
type Codec uint8

const (
	CodecUnspecified = iota
	CodecRaw
	CodecGzip
	CodecLzop
	CodecZstd

	CodecAuto = -1
)

func (c Codec) String() string {
	switch c {
	case CodecUnspecified:
		return unspecifiedLabel
	case CodecRaw:
		return "Raw"
	case CodecGzip:
		return "Gzip"
	case CodecLzop:
		return "Lzop"
	case CodecZstd:
		return "Zstd"
	default:
		return unknownLabel
	}
}
