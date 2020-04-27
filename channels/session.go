package channels

type Session interface {
	Push(interface{}, ...int) error
	QueueNames() []string
	PushByIdx(int, interface{})
	Streams() ([]chan interface{}, error)
}
