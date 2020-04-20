package transfer

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/panjf2000/ants/v2"
)

var (
	workerModify = map[string]func(*Worker) error{
		"base": func(w *Worker) error {
			return nil
		},
	}
	errInputType            = errors.New("输入参数不是[]event.EventV2类型")
	errOutOfIndex           = errors.New("下标越界")
	errWorkerTypeNotDefined = errors.New("未定义的transfer类型")
	errDBNotDefined         = errors.New("没定义主库链接")
)

func RegisterWorkerModify(name string, f func(*Worker) error) {
	workerModify[name] = f
}

type TransferRuler func([]event.EventV2) (interface{}, error)

func (this TransferRuler) Run(i interface{}) (interface{}, error) {
	input, ok := i.([]event.EventV2)
	if !ok {
		return nil, errInputType
	}
	return this(input)
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
	if err = ret.modifyWithUser(); err != nil {
		return nil, err
	}
	if err = ret.setPool(); err != nil {
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
	Data []event.EventV2
	Idx  int
}

func (this *Worker) AddAction(f func([]event.EventV2) (interface{}, error)) {
	this.Worker.AddAction(TransferRuler(f))
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
		this.transfer,
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
