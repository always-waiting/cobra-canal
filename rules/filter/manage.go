package filter

import (
	"context"
	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/streadway/amqp"
)

type Manager struct {
	*rules.Manager
	aggregator *collection.Aggregator
	worker     *Worker
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
	if err = ret.SetAggregator(); err != nil {
		return
	}
	if err = ret.SetNextManager(config.TransferWorker); err != nil {
		return
	}
	if err = ret.SetWorker(); err != nil {
		return
	}
	return
}

func (this *Manager) SetWorker() (err error) {
	this.worker, err = CreateWorker(this)
	return
}

func (this *Manager) SetAggregator() (err error) {
	this.aggregator, err = this.Cfg.Aggregator()
	return
}

func (this *Manager) Start() error {
	go this.ErrSend()
	if this.aggregator != nil {
		this.Wg.Add(1)
		go this.Aggregator()
	}
	return this.Receive()
}

func (this *Manager) Aggregator() {
	defer func() {
		this.Log.Debug("聚合逻辑退出")
		this.Wg.Done()
	}()
	out := this.aggregator.Collection()
	for {
		select {
		case <-this.Ctx.Done():
			return
		case ele := <-out:
			this.Log.Infof("把事件组%s发送到数据转换池", ele.Key)
			if err := this.Next.Push(ele.Events); err != nil {
				this.Log.Infof("发送数据转换池失败: %s", err)
				this.ErrPush(err)
			}
		}
	}
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
		for _, in := range instreams {
			this.Wg.Add(1)
			go this.Consume(in, ctx)
		}
		select {
		case <-this.Ctx.Done():
			cancel()
			return nil
		case <-this.ReConnSignal():
			count = 0
			cancel()
		}

	}
}

func (this *Manager) Consume(in <-chan amqp.Delivery, ctx context.Context) {
	defer func() {
		this.Log.Debug("消费逻辑退出")
		this.Wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case info := <-in:
			if len(info.Body) == 0 {
				this.Ack(info.DeliveryTag, false)
				continue
			}
			e := event.EventV2{}
			if this.Cfg.Compress {
				if err := e.Decompress(info.Body); err != nil {
					this.Ack(info.DeliveryTag, false)
					this.ErrPush(err)
					continue
				}
			} else {
				if err := e.FromJSON(info.Body); err != nil {
					this.Ack(info.DeliveryTag, false)
					this.ErrPush(err)
					continue
				}
			}
			if err := this.Ack(info.DeliveryTag, false); err != nil {
				this.ErrPush(err)
			}
			this.Log.Debugf("获取事件:%s\n", e)
			if err := this.worker.Invoke(&e); err != nil {
				this.ErrPush(err)
			}
		}
	}
}

func (this *Manager) Close() {
	this.Log.Info("关闭manager")
	this.Manager.SessClose()
	if this.aggregator != nil {
		this.Log.Info("关闭聚合器逻辑开始")
		this.aggregator.Close()
	}
	this.Manager.Close()
}
