package consumes

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
	Reset() error
	GetName() string
	SetLogger(*log.Logger)
	SetRuleNum(int)
	GetRuleNum() int
	IsClosed() bool
	ConsumerInfo() (ConsumerInfo, error)
}

type ConsumerInfo struct {
	Name      string      `json:"-"`
	Id        int         `json:"id"`
	Closed    bool        `json:"closed"`
	ExtraInfo interface{} `json:"extra_info,omitempty"`
}
