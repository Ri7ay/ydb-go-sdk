package topic

type ReadStream interface {
	Send(ReadSendMessage) error
	Recv() (ReadRecvMessage, error)
	CloseSend() error
	Close() error
}

// ReadSendMessage is group of types sutable for send to ReadStream.Send
type ReadSendMessage interface {
	isReadRequest()
}
