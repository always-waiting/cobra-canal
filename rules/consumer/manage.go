package consumer

import (
	"context"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rules"
)

type Manager struct {
	*rules.Manager
	workers []*Worker
}

func CreateManager(rule config.RuleConfigV2) (ret *Manager, err error) {
	base, err := rules.CreateManager(rule, config.ConsumeWorker)
	if err != nil {
		return
	}
	ret = &Manager{Manager: base}
	return
}

func CreateManagerWithNext(rule config.RuleConfigV2) (ret *Manager, err error) {
	if ret, err = CreateManager(rule); err != nil {
		return
	}
	if err = ret.SetWorker(); err != nil {
		return
	}
	return
}

func (this *Manager) SetWorker() (err error) {
	this.workers, err = CreateWorkers(this)
	return
}

func (this *Manager) Start() error {
	go this.ErrSend()
	return this.Receive()
}

func (this *Manager) Receive() error {
	count := 0
	for {
		instreams, err := this.StreamAll()
		if err != nil {
			if count > 20 {
				return errors.Errorf("获取数据转换队列失败: %s", err)
			}
			count++
			this.Log.Info("获取数据转换队列失败...")
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		for idx, in := range instreams {
			this.Wg.Add(1)
			go this.Consume(idx, in, ctx)
		}
		select {
		case <-this.Ctx.Done():
			cancel()
			return nil
		}
	}
}

func (this *Manager) Consume(idx int, in <-chan interface{}, ctx context.Context) {
	defer func() {
		this.Log.Debug("消费逻辑退出")
		this.Wg.Done()
	}()
	if len(this.workers) <= idx {
		return
	}
	worker := this.workers[idx]
	for {
		select {
		case <-ctx.Done():
			return
		case info := <-in:
			this.Log.Debugf("消费器%d: 获取消费事件: %v\n", idx, info)
			if err := worker.Invoke(Param{Data: info, Idx: idx}); err != nil {
				this.ErrPush(err)
			}
		}

	}
}

func (this *Manager) Close() {
	this.Log.Info("关闭consume manager")
	this.Manager.SessClose()
	this.Manager.Close()
}
