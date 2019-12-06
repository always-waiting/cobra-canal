package consumer

import (
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

const (
	INFO_HEADER        = "<----%s---->"
	ERR_MERGE          = "%s\n%s"
	BASE_CONSUMER_ERR1 = "未定义数据转换器"
)

type BaseConsumer struct {
	name         string
	number       int
	transferFunc func([]event.Event) (interface{}, error)
	Log          *log.Logger
}

func (b *BaseConsumer) SetLogger(l *log.Logger) {
	b.Log = l
}

func (b *BaseConsumer) SetNumber(i int) {
	b.number = i
}

func (b *BaseConsumer) Number() int {
	return b.number
}

func (b *BaseConsumer) GetName() string {
	return b.name
}

func (b *BaseConsumer) SetName(name string) {
	b.name = name
}

func (b *BaseConsumer) Open() error {
	return nil
}

func (b *BaseConsumer) Close() error {
	return nil
}

func (b *BaseConsumer) Transfer(events []event.Event) (interface{}, error) {
	if b.transferFunc != nil {
		return b.transferFunc(events)
	}
	return events, nil
	//return nil, errors.New(BASE_CONSUMER_ERR1)
}

func (b *BaseConsumer) SetTransferFunc(f func([]event.Event) (interface{}, error)) {
	b.transferFunc = f
}

func (b *BaseConsumer) Solve(data interface{}) error {
	return nil
}

func (b BaseConsumer) MergeErr(err1, err2 error) (retErr error) {
	if err2 == nil {
		retErr = err1
	} else {
		if err1 == nil {
			retErr = err2
		} else {
			retErr = errors.Errorf(ERR_MERGE, err1.Error(), err2.Error())
		}
	}
	return
}
