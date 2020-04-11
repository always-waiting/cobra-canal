package rules

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/panjf2000/ants/v2"
)

var (
	errOutOfIndex           = errors.New("下标越界，没有对应action")
	errWorkerTypeNotDefined = errors.New("未定义的过滤类型")
)

type Action interface {
	Run(interface{}) (interface{}, error)
}

type Worker struct {
	acts []Action
	pool *ants.PoolWithFunc
	WCfg config.WorkerConfig
}

func CreateWorker(wCfg config.WorkerConfig) (ret *Worker, err error) {
	ret = &Worker{WCfg: wCfg}
	return
}

func (this *Worker) Free() int {
	return this.pool.Free()
}

func (this *Worker) Running() int {
	return this.pool.Running()
}

func (this *Worker) Release() {
	this.pool.Release()
}

func (this *Worker) ParseWorker(definedTypeMap map[string][]Action) (err error) {
	if acts, ok := definedTypeMap[this.WCfg.TypeName()]; !ok {
		err = errWorkerTypeNotDefined
		return
	} else {
		this.acts = acts
	}
	return
}

func (this *Worker) SetPool(f func(interface{}), opts ...ants.Option) (err error) {
	size := this.WCfg.MaxNum()
	this.pool, err = ants.NewPoolWithFunc(
		size, f,
		opts...,
	)
	return
}

func (this *Worker) AddAction(f Action) {
	this.acts = append(this.acts, f)
}

func (this *Worker) DelAction(idx int) {
	if idx >= len(this.acts) {
		return
	}
	tail := this.acts[idx+1:]
	head := this.acts[0:idx]
	if len(tail) != 0 {
		head = append(head, tail...)
	}
	this.acts = head
}

func (this *Worker) Action(idx int) (ret Action, err error) {
	if idx >= len(this.acts) {
		err = errOutOfIndex
		return
	}
	for i, act := range this.acts {
		if idx == i {
			ret = act
			break
		}
	}
	return
}

func (this *Worker) Actions() []Action {
	return this.acts
}

func (this *Worker) Invoke(i interface{}) error {
	return this.pool.Invoke(i)
}
