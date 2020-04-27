package consumer

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/panjf2000/ants/v2"
)

var (
	workerModify = map[string]func(*Worker) error{
		"base": func(w *Worker) error {
			w.AddAction(func(input interface{}) error {
				w.manager.Log.Debugf("消费器%d: 默认消费模式: %#v", w.Id, input)
				return nil
			})
			return nil
		},
	}
	errInputType            = errors.New("输入参数不是[]event.EventV2类型")
	errOutOfIndex           = errors.New("下标越界")
	errWorkerTypeNotDefined = errors.New("未定义的consumer类型")
	errDBNotDefined         = errors.New("没定义主库链接")
)

func RegisterWorkerModify(name string, f func(*Worker) error) {
	workerModify[name] = f
}

type ConsumeRuler func(interface{}) error

func (this ConsumeRuler) Run(i interface{}) (interface{}, error) {
	err := this(i)
	return i, err
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
		Worker:  &rules.Worker{WCfg: WCfg, Id: idx},
		manager: manager,
	}
	if err = ret.modifyWithUser(); err != nil {
		return nil, err
	}
	if err = ret.setPool(); err != nil {
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
	for idx, wCfg := range wCfgs {
		wcfg := wCfg
		worker := &Worker{
			Worker:  &rules.Worker{WCfg: wcfg, Id: idx},
			manager: manager,
		}
		if err := worker.modifyWithUser(); err != nil {
			return nil, err
		}
		if err := worker.setPool(); err != nil {
			return nil, err
		}
		ret = append(ret, worker)
	}
	return
}

type Param struct {
	Data interface{}
	Idx  int
}

func (this *Worker) consume(i interface{}) {
	params := i.(Param)
	acts := this.Actions()
	if len(acts) == 0 {
		this.manager.Log.Debugf("消费器%d: 默认消费模式: %#v", params.Idx, params.Data)
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

func (this *Worker) AddAction(f func(interface{}) error) {
	this.Worker.AddAction(ConsumeRuler(f))
}

func (this *Worker) modifyWithUser() error {
	modifyFunc, ok := workerModify[this.WCfg.TypeName()]
	if !ok {
		return errWorkerTypeNotDefined
	}
	if err := modifyFunc(this); err != nil {
		return err
	}
	return nil
}

func (this *Worker) setPool() error {
	return this.SetPool(
		this.consume,
		ants.WithPanicHandler(func(i interface{}) {
			this.manager.ErrPush(i)
		}),
	)
}

func (this *Worker) Info(input string) {
	this.manager.Log.Info(input)
}

func (this *Worker) Debugf(tpl string, input ...interface{}) {
	this.manager.Log.Debugf(tpl, input...)
}

func (this *Worker) DbAddr() (string, error) {
	if !this.DbRequired() {
		return "", errDBNotDefined
	}
	return this.manager.Cfg.DbCfg.ToGormAddr()
}
