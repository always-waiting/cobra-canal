package filter

import (
	"errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/panjf2000/ants/v2"
)

var (
	workerTypeMap = map[string][]FilterRuler{
		"base": []FilterRuler{},
	}
	errWorkerTypeNotDefined = errors.New("未定义的过滤类型")
)

type FilterRuler func(*event.EventV2) (bool, error)

type Worker struct {
	rules   []FilterRuler
	pool    *ants.PoolWithFunc
	manager *Manager
}

func CreateWorker(manager *Manager) (ret *Worker, err error) {
	ret = &Worker{manager: manager}
	size := manager.Cfg.FilterWorker().MaxNum()
	if err = ret.ParseWorker(); err != nil {
		return
	}
	ret.pool, err = ants.NewPoolWithFunc(
		size,
		ret.filter,
		ants.WithPanicHandler(func(i interface{}) {
			manager.errHr.Push(i)
		}),
	)
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

func (this *Worker) ParseWorker() (err error) {
	workerCfg := this.manager.Cfg.FilterWorker()
	if rules, ok := workerTypeMap[workerCfg.TypeName()]; !ok {
		err = errWorkerTypeNotDefined
	} else {
		this.rules = rules
	}
	return
}

func (this *Worker) AddRule(f FilterRuler) {
	this.rules = append(this.rules, f)
}

func (this *Worker) DelRule(idx int) {
	if idx >= len(this.rules) {
		return
	}
	tail := this.rules[idx+1:]
	head := this.rules[0:idx]
	if len(tail) != 0 {
		head = append(head, tail...)
	}
	this.rules = head
}

func (this *Worker) filter(i interface{}) {
	e := i.(*event.EventV2)
	for _, f := range this.rules {
		ret, err := f(e)
		if err != nil {
			this.manager.errHr.Push(err)
			return
		}
		if !ret {
			e.SetPass(ret)
			return
		}
	}
	e.SetPass(true)
}

func (this *Worker) Analyze(e event.EventV2) (ret bool) {
	e.CreatePass()
	this.pool.Invoke(&e)
	return e.Pass()
}
