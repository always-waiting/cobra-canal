package consumer

import (
	"github.com/always-waiting/cobra-canal/event"
	"github.com/siddontang/go-log/log"
)

type Consumer interface {
	Transfer([]event.Event) (interface{}, error)
	Solve(interface{}) error
	SetTransferFunc(func([]event.Event) (interface{}, error))
	SetNumber(int)
	Number() int
	Open() error
	Close() error
	GetName() string
	SetLogger(*log.Logger)
}
