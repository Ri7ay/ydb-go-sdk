package persqueue

// Codec for stream data encoding settings
type Codec uint8

const (
	CodecUnspecified = iota
	CodecRaw
	CodecGzip
	CodecLzop
	CodecZstd
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

// Format for messages data encoding format in stream
type Format uint8

const (
	FormatUnspecified = iota
	FormatBase
)

func (f Format) String() string {
	switch f {
	case FormatUnspecified:
		return unspecifiedLabel
	case FormatBase:
		return "Base"
	default:
		return unknownLabel
	}
}

const (
	unspecifiedLabel = "Unspecified"
	unknownLabel     = "Unknown"
)