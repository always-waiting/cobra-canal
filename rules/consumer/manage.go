package consumer

import (
	"bytes"
	"compress/flate"
	"context"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/streadway/amqp"
	"io"
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
		case <-this.ReConnSignal():
			count = 0
			cancel()
		}
	}
}

func (this *Manager) Consume(idx int, in <-chan amqp.Delivery, ctx context.Context) {
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
			if len(info.Body) == 0 {
				this.Ack(info.DeliveryTag, false)
				continue
			}
			var input []byte
			if this.Cfg.Compress {
				inbuffer := bytes.NewBuffer(info.Body)
				flateReader := flate.NewReader(inbuffer)
				defer flateReader.Close()
				out := bytes.NewBuffer(nil)
				io.Copy(out, flateReader)
				input = out.Bytes()
			} else {
				input = info.Body
			}
			if err := this.Ack(info.DeliveryTag, false); err != nil {
				this.ErrPush(err)
			}
			this.Log.Debugf("消费器%d: 获取消费事件: %v\n", idx, string(input))
			if err := worker.Invoke(Param{Data: input, Idx: idx}); err != nil {
				this.ErrPush(err)
			}
		}

	}
}

func (this *Manager) Close() {
	this.Log.Info("关闭manager")
	this.Manager.SessClose()
	this.Manager.Close()
}
