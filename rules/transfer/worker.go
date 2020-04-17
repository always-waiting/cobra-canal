package transfer

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
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

func AddAction(name string, f func([]event.EventV2) (interface{}, error)) {
	if acts, ok := workerTypeMap[name]; !ok {
		workerTypeMap[name] = []rules.Action{TransferRuler(f)}
	} else {
		acts = append(acts, TransferRuler(f))
		workerTypeMap[name] = acts
	}
}

type TransferRuler func([]event.EventV2) (interface{}, error)

func (this TransferRuler) Run(i interface{}) (interface{}, error) {
	input, ok := i.([]event.EventV2)
	if !ok {
		return nil, errInputType
	}
	return this(input)
}

func AddTransferRuler(name string, f TransferRuler) {
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
	WCfg, err := manager.Cfg.Worker(config.TransferWorker, idx)
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
		ret.transfer,
		ants.WithPanicHandler(func(i interface{}) {
			ret.manager.ErrPush(i)
		}),
	); err != nil {
		return nil, err
	}
	return
}

func CreateWorkers(manager *Manager) (ret []*Worker, err error) {
	wCfgs, err := manager.Cfg.Workers(config.TransferWorker)
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
			worker.transfer,
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
	Data []event.EventV2
	Idx  int
}

func (this *Worker) transfer(i interface{}) {
	params := i.(Param)
	acts := this.Actions()
	var ret, input interface{}
	var err error
	if len(acts) == 0 {
		if ret, err = event.ToJSON(params.Data); err != nil {
			this.manager.ErrPush(err)
			return
		}
	} else {
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
	this.manager.Log.Debugf("转换器%d: 向消费池推送数据", params.Idx)
	this.manager.Next.PushByIdx(params.Idx, ret)
	this.manager.Log.Debugf("转换器%d: 推送成功", params.Idx)
}
