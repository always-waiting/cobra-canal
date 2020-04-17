package consumer

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/panjf2000/ants/v2"
)

var (
	workerTypeMap = map[string][]rules.Action{
		"base": []rules.Action{},
	}
	errInputType  = errors.New("输入参数不是[]event.EventV2类型")
	errOutOfIndex = errors.New("下标越界")
)

func AddAction(name string, f func([]byte) error) {
	if acts, ok := workerTypeMap[name]; !ok {
		workerTypeMap[name] = []rules.Action{ConsumeRuler(f)}
	} else {
		acts = append(acts, ConsumeRuler(f))
		workerTypeMap[name] = acts
	}
}

type ConsumeRuler func([]byte) error

func (this ConsumeRuler) Run(i interface{}) (interface{}, error) {
	input, ok := i.([]byte)
	if !ok {
		return nil, errInputType
	}
	err := this(input)
	return nil, err
}

func AddConsumeRuler(name string, f ConsumeRuler) {
	if acts, ok := workerTypeMap[name]; ok {
		acts = append(acts, f)
		workerTypeMap[name] = acts
	} else {
		workerTypeMap[name] = []rules.Action{f}
	}
}

type Worker struct {
	*rules.Worker
	manager *Manager
}

func CreateWorker(manager *Manager, idx int) (ret *Worker, err error) {
	WCfg, err := manager.Cfg.Worker(config.ConsumeWorker, idx)
	if err != nil {
		return
	}
	ret = &Worker{
		Worker:  &rules.Worker{WCfg: WCfg},
		manager: manager,
	}
	if err = ret.ParseWorker(workerTypeMap); err != nil {
		return nil, err
	}
	if err = ret.SetPool(
		ret.consume,
		ants.WithPanicHandler(func(i interface{}) {
			ret.manager.ErrPush(i)
		}),
	); err != nil {
		return nil, err
	}
	return
}

func CreateWorkers(manager *Manager) (ret []*Worker, err error) {
	wCfgs, err := manager.Cfg.Workers(config.ConsumeWorker)
	if err != nil {
		return
	}
	ret = make([]*Worker, 0)
	for _, wCfg := range wCfgs {
		wcfg := wCfg
		worker := &Worker{
			Worker:  &rules.Worker{WCfg: wcfg},
			manager: manager,
		}
		if err = worker.ParseWorker(workerTypeMap); err != nil {
			return nil, err
		}
		if err = worker.SetPool(
			worker.consume,
			ants.WithPanicHandler(func(i interface{}) {
				worker.manager.ErrPush(i)
			}),
		); err != nil {
			return nil, err
		}
		ret = append(ret, worker)
	}
	return
}

type Param struct {
	Data []byte
	Idx  int
}

func (this *Worker) consume(i interface{}) {
	params := i.(Param)
	acts := this.Actions()
	if len(acts) == 0 {
		this.manager.Log.Debugf("消费器%d: 默认消费模式: %s", params.Idx, string(params.Data))
		return
	}
	var ret, input interface{}
	var err error
	input = params.Data
	for _, act := range acts {
		ret, err = act.Run(input)
		if err != nil {
			this.manager.ErrPush(err)
			return
		}
		input = ret
	}
}
