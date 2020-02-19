package consumes

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"runtime"
)

const (
	INFO_HEADER        = "<----%s---->"
	ERR_MERGE          = "%s\n%s"
	BASE_CONSUMER_ERR1 = "未定义数据转换器"
)

type BaseConsumer struct {
	name         string
	number       int
	rulerNum     int
	transferFunc func([]event.Event) (interface{}, error)
	solveFunc    func(interface{}) error
	Log          *log.Logger
	closed       bool
}

func (b *BaseConsumer) Debugf(tmp string, i ...interface{}) {
	nTmp := fmt.Sprintf("Rule%d-Csr%d: %s", b.rulerNum, b.number, tmp)
	b.Log.Debugf(nTmp, i...)
}

func (b *BaseConsumer) Infof(tmp string, i ...interface{}) {
	nTmp := fmt.Sprintf("Rule%d-Csr%d: %s", b.rulerNum, b.number, tmp)
	b.Log.Infof(nTmp, i...)
}

func (b *BaseConsumer) Errorf(tmp string, i ...interface{}) {
	nTmp := fmt.Sprintf("Rule%d-Csr%d: %s", b.rulerNum, b.number, tmp)
	b.Log.Errorf(nTmp, i...)
}

func (b *BaseConsumer) Info(i string) {
	b.Log.Infof("Rule%d-Csr%d: %s", b.rulerNum, b.number, i)
}

func (b *BaseConsumer) Debug(i string) {
	b.Log.Debugf("Rule%d-Csr%d: %s", b.rulerNum, b.number, i)
}

func (b *BaseConsumer) Error(i string) {
	b.Log.Errorf("Rule%d-Csr%d: %s", b.rulerNum, b.number, i)
}

func (b *BaseConsumer) SetRuleNum(i int) {
	b.rulerNum = i
}

func (b *BaseConsumer) GetRuleNum() int {
	return b.rulerNum
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
	b.closed = true
	return nil
}

func (b *BaseConsumer) Reset() error {
	b.closed = false
	return nil
}

func (b *BaseConsumer) Transfer(events []event.Event) (data interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			err = errors.Errorf("消费器Transfer未知错误:%v\n%s", e, string(buf[:n]))
		}
	}()
	if b.transferFunc != nil {
		return b.transferFunc(events)
	}
	return events, nil
	//return nil, errors.New(BASE_CONSUMER_ERR1)
}

func (b *BaseConsumer) SetTransferFunc(f func([]event.Event) (interface{}, error)) {
	b.transferFunc = f
}

func (b *BaseConsumer) SetSolveFunc(f func(interface{}) error) {
	b.solveFunc = f
}

func (b *BaseConsumer) Solve(data interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.Errorf("消费器Solve未知错误:%v", e)
		}
	}()
	if b.solveFunc != nil {
		return b.solveFunc(data)
	}
	return
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

func (b *BaseConsumer) IsClosed() bool {
	return b.closed
}
