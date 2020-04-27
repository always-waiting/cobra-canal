package filter

import (
	"github.com/always-waiting/cobra-canal/collection"
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
	errInputType            = errors.New("输入参数不是*event.EventV2类型")
	errWorkerTypeNotDefined = errors.New("未定义的filter类型")
	errDBNotDefined         = errors.New("没定义主库链接")
)

func RegisterWorkerModify(name string, f func(*Worker) error) {
	workerModify[name] = f
}

type FilterRuler func(*event.EventV2) (bool, error)

func (this FilterRuler) Run(i interface{}) (interface{}, error) {
	e, ok := i.(*event.EventV2)
	if !ok {
		return nil, errInputType
	}
	return this(e)
}

type Worker struct {
	*rules.Worker
	manager    *Manager
	aggregator *collection.Aggregator
}

func defaultModify(w *Worker) error {
	if !w.WCfg.HasTableFilter() {
		return nil
	}
	tf, err := w.WCfg.TableFilter()
	if err != nil {
		return err
	}
	w.AddAction(func(e *event.EventV2) (bool, error) {
		return tf.IsTablePass(e.Table.Schema, e.Table.Name), nil
	})
	return nil
}

func CreateWorker(manager *Manager, idx int) (ret *Worker, err error) {
	wCfg, err := manager.Cfg.Worker(config.FilterWorker, idx)
	if err != nil {
		return
	}
	ret = &Worker{
		Worker:  &rules.Worker{WCfg: wCfg, Id: idx},
		manager: manager,
	}
	if err := ret.modifyWithUser(); err != nil {
		return nil, err
	}
	if err := ret.setPool(); err != nil {
		return nil, err
	}
	if err := ret.setAggregator(); err != nil {
		return nil, err
	}
	return
}

func CreateWorkers(manager *Manager) (ret []*Worker, err error) {
	wCfgs, err := manager.Cfg.Workers(config.FilterWorker)
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
		if err = worker.modifyWithUser(); err != nil {
			return nil, err
		}
		if err = worker.setPool(); err != nil {
			return nil, err
		}
		if err = worker.setAggregator(); err != nil {
			return nil, err
		}
		ret = append(ret, worker)
	}
	return
}

func (this *Worker) setAggregator() error {
	var err error
	this.aggregator, err = this.WCfg.Aggregator()
	go this.Aggregator()
	return err
}

func (this *Worker) Aggregator() {
	out := this.aggregator.Collection()
	for {
		select {
		case <-this.manager.Ctx.Done():
			return
		case ele := <-out:
			this.manager.Log.Infof("把事件组%s发送到数据转换池", ele.Key)
			if this.Id == len(this.manager.workers)-1 {
				ids := make([]int, 0)
				for idx, _ := range this.manager.Next.QueueNames() {
					if idx >= this.Id {
						ids = append(ids, idx)
					}
				}
				if err := this.manager.Next.Push(ele.Events, ids...); err != nil {
					this.manager.Log.Infof("发送数据转换池失败: %s", err)
					this.manager.ErrPush(err)
				}
			} else {
				if err := this.manager.Next.PushByIdx(this.Id, ele.Events); err != nil {
					this.manager.Log.Infof("发送数据转换池失败: %s", err)
					this.manager.ErrPush(err)
				}
			}
		}
	}
}

func (this *Worker) Close() {
	this.aggregator.Close()
}

func (this *Worker) setPool() error {
	return this.SetPool(
		this.filter,
		ants.WithPanicHandler(func(i interface{}) {
			this.manager.ErrPush(i)
		}),
	)
}

func (this *Worker) modifyWithUser() error {
	if err := defaultModify(this); err != nil {
		return err
	}
	modifyFunc, ok := workerModify[this.WCfg.TypeName()]
	if !ok {
		return errWorkerTypeNotDefined
	}
	if err := modifyFunc(this); err != nil {
		return err
	}
	return nil
}

func (this *Worker) AddAction(f func(*event.EventV2) (bool, error)) {
	this.Worker.AddAction(FilterRuler(f))
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

func (this *Worker) filter(i interface{}) {
	e := i.(*event.EventV2)
	var flag bool
	for _, act := range this.Actions() {
		ret, err := act.Run(e)
		if err != nil {
			this.manager.ErrPush(err)
			return
		}
		flag = ret.(bool)
		if !flag {
			this.manager.Log.Info("不符合条件，忽略事件")
			return
		}
	}
	if this.aggregator != nil {
		if key, err := this.aggregator.Add(*e); err != nil {
			this.manager.Log.Debugf("事件(%s)聚合出错: %s", e, err)
			this.manager.ErrPush(err)
		} else {
			this.manager.Log.Debugf("事件聚合到%s键中", key)
		}
	} else {
		if this.Id == len(this.manager.workers)-1 {
			ids := make([]int, 0)
			for idx, _ := range this.manager.Next.QueueNames() {
				if idx >= this.Id {
					ids = append(ids, idx)
				}
			}
			if err := this.manager.Next.Push([]event.EventV2{*e}, ids...); err != nil {
				this.manager.Log.Infof("发送数据转换池失败: %s", err)
				this.manager.ErrPush(err)
			}
		} else {
			if err := this.manager.Next.PushByIdx(this.Id, []event.EventV2{*e}); err != nil {
				this.manager.Log.Infof("发送数据转换池失败: %s", err)
				this.manager.ErrPush(err)
			}
		}
	}
}
