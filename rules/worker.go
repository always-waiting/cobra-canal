package rules

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/panjf2000/ants/v2"
	"strings"
)

var (
	errOutOfIndex           = errors.New("下标越界，没有对应action")
	errWorkerTypeNotDefined = errors.New("未定义的过滤类型")
	errCfgNotDefined        = "配置%s不存在"
	errNotString            = "不能转换为string"
)

type Action interface {
	Run(interface{}) (interface{}, error)
}

type Worker struct {
	acts []Action
	pool *ants.PoolWithFunc
	WCfg config.WorkerConfig
	Id   int
}

func CreateWorker(wCfg config.WorkerConfig) (ret *Worker, err error) {
	ret = &Worker{WCfg: wCfg}
	return
}

func (this *Worker) GetConfig(key string) (interface{}, error) {
	keys := strings.Split(key, ".")
	cfg := this.WCfg
	var ret interface{}
	for idx, keyName := range keys {
		if info, ok := cfg[keyName]; !ok {
			return nil, errors.Errorf(errCfgNotDefined, keyName)
		} else {
			if idx == len(keys)-1 {
				ret = info
			} else {
				var ok bool
				cfg, ok = info.(map[string]interface{})
				if !ok {
					return nil, errors.Errorf(errCfgNotDefined, keys[idx+1])
				}
			}
		}
	}
	return ret, nil
}

func (this *Worker) GetStrCfg(key string) (string, error) {
	i, err := this.GetConfig(key)
	if err != nil {
		return "", err
	}
	ret, ok := i.(string)
	if !ok {
		return "", errors.Errorf(errNotString)
	}
	return ret, nil
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

func (this *Worker) TypeName() string {
	return this.WCfg.TypeName()
}

func (this *Worker) DbRequired() bool {
	return this.WCfg.DbRequired()
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
	if this.acts == nil {
		return []Action{}
	}
	return this.acts
}

func (this *Worker) Invoke(i interface{}) error {
	return this.pool.Invoke(i)
}
