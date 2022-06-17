package topic

const (
	unspecifiedLabel = "Unspecified"
	unknownLabel     = "Unknown"
)

type PartitionStreamStatus uint8

const (
	PartitionStreamUnscpecified = iota
	PartitionStreamCreating
	PartitionStreamDestroying
	PartitionStreamReading
	PartitionStreamStopped
)

func (s PartitionStreamStatus) String() string {
	switch s {
	case PartitionStreamUnscpecified:
		return unspecifiedLabel
	case PartitionStreamCreating:
		return "Creating"
	case PartitionStreamDestroying:
		return "Destroying"
	case PartitionStreamReading:
		return "Reading"
	case PartitionStreamStopped:
		return "Stopped"
	default:
		return unknownLabel
	}
}
