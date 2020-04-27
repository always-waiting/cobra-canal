package filter

import (
	"context"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rules"
	"sync"
)

type Manager struct {
	*rules.Manager
	workers []*Worker
}

func CreateManager(rule config.RuleConfigV2) (ret *Manager, err error) {
	baseManager, err := rules.CreateManager(rule, config.FilterWorker)
	if err != nil {
		return
	}
	ret = &Manager{Manager: baseManager}
	return
}

func CreateManagerWithNext(rule config.RuleConfigV2) (ret *Manager, err error) {
	if ret, err = CreateManager(rule); err != nil {
		return
	}
	if err = ret.SetNextManager(config.ConsumeWorker); err != nil {
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
				return errors.Errorf("获取过滤池队列失败: %s", err)
			}
			count++
			this.Log.Info("获取过滤池队列失败...")
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
			e := info.(event.EventV2)
			this.Log.Debugf("获取事件:%s\n", e)
			if err := worker.Invoke(&e); err != nil {
				this.ErrPush(err)
			}
		}
	}
}

func (this *Manager) Close() {
	this.Log.Info("关闭manager")
	this.Manager.SessClose()
	wg := sync.WaitGroup{}
	for _, worker := range this.workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.Close()
		}(worker)
	}
	wg.Wait()
	this.Manager.Close()
}
