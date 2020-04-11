package filter

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
	errInputType = errors.New("输入参数不是*event.EventV2类型")
)

type FilterRuler func(*event.EventV2) (bool, error)

func (this FilterRuler) Run(i interface{}) (interface{}, error) {
	e, ok := i.(*event.EventV2)
	if !ok {
		return nil, errInputType
	}
	return this(e)
}

func AddFilterRuler(name string, f FilterRuler) {
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

func CreateWorker(manager *Manager) (ret *Worker, err error) {
	wCfg, err := manager.Cfg.Worker(config.FilterWorker, 0)
	if err != nil {
		return
	}
	ret = &Worker{
		Worker:  &rules.Worker{WCfg: wCfg},
		manager: manager,
	}
	if err = ret.ParseWorker(workerTypeMap); err != nil {
		return nil, err
	}
	if err = ret.SetPool(
		ret.filter,
		ants.WithPanicHandler(func(i interface{}) {
			ret.manager.ErrPush(i)
		}),
	); err != nil {
		return nil, err
	}
	return
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
	if this.manager.aggregator != nil {
		if key, err := this.manager.aggregator.Add(*e); err != nil {
			this.manager.Log.Debugf("事件(%s)聚合出错: %s", e, err)
			this.manager.ErrPush(err)
		} else {
			this.manager.Log.Debugf("事件聚合到%s键中", key)
		}
	} else {
		this.manager.Next.Push([]event.EventV2{*e})
	}
}
