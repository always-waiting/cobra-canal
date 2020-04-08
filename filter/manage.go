package filter

import (
	"context"
	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rabbitmq"
	"github.com/always-waiting/cobra-canal/transfer"
	"github.com/siddontang/go-log/log"
	"github.com/streadway/amqp"
	"sync"
)

const (
	EXCHANGE_NAME = "cobra_filter"
)

func CreateManager(rule config.RuleConfigV2) (ret *Manager, err error) {
	ret = &Manager{
		Cfg: rule,
	}
	ret.Ctx, ret.cancel = context.WithCancel(context.Background())
	if err = ret.SetLogger(); err != nil {
		return
	}
	if _, err = ret.SetSession(); err != nil {
		return
	}
	if err = ret.SetErrHr(); err != nil {
		return
	}
	return
}

type Manager struct {
	wg         sync.WaitGroup
	Cfg        config.RuleConfigV2
	sess       *rabbitmq.Session
	Log        *log.Logger
	Next       *transfer.Manager
	errHr      *cobraErrors.ErrHandlerV2
	worker     *Worker
	aggregator *collection.Aggregator
	Ctx        context.Context
	cancel     context.CancelFunc
}

func (this *Manager) Name() string {
	return this.Cfg.FilterManagerName()
}

func (this *Manager) IsTablePass(e event.EventV2) bool {
	if this.Cfg.FilterManage.HasTableFilter() {
		tafilter := this.Cfg.FilterManage.TableFilterCfg
		return tafilter.IsTablePass(e.Table.Schema, e.Table.Name)
	}
	return true
}

func (this *Manager) Push(e event.EventV2) (err error) {
	var info []byte
	if this.Cfg.Compress {
		info, err = e.Compress()
	} else {
		info, err = e.ToJSON()
	}
	if err != nil {
		return
	}
	return this.sess.Push(info)
}

func (this *Manager) SetWorker() (err error) {
	this.worker, err = CreateWorker(this)
	return
}

func (this *Manager) SetErrHr() (err error) {
	eHr := this.Cfg.ErrCfg.MakeHandler()
	this.errHr = &eHr
	return
}

func (this *Manager) SetLogger() (err error) {
	logger, err := this.Cfg.GetFilterManagerLogger()
	this.Log = logger
	return
}

func (this *Manager) SetSession() (*rabbitmq.Session, error) {
	filterName := this.Cfg.FilterWorkerName()
	sess, err := rabbitmq.New(EXCHANGE_NAME, this.Cfg.QueueAddr, filterName)
	sess.Log = this.Log
	this.sess = sess
	return sess, err
}

func (this *Manager) SetNextManager() (err error) {
	this.Next, err = transfer.CreateManager(this.Cfg)
	return
}

func (this *Manager) SetAggregator() (err error) {
	this.aggregator, err = this.Cfg.Aggregator()
	return
}

func (this *Manager) Start() {
	go this.errHr.Send()
	if this.aggregator != nil {
		this.wg.Add(1)
		go this.Aggregator()
	}
	go this.Receive()
}

func (this *Manager) Receive() error {
	count := 0
	for {
		instreams, err := this.sess.StreamAll()
		if err != nil {
			if count > 20 {
				return cobraErrors.Errorf("获取过滤池队列失败: %s", err)
			}
			count++
			this.Log.Info("获取过滤池队列失败...")
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		for _, in := range instreams {
			go this.Consume(in, ctx)
		}
		select {
		case <-this.Ctx.Done():
			cancel()
			return nil
		case <-this.sess.ReChanSignal:
			count = 0
			cancel()
		}

	}
}

func (this *Manager) Aggregator() {
	defer func() {
		this.Log.Debug("聚合逻辑退出")
		this.wg.Done()
	}()
	out := this.aggregator.Collection()
	for {
		select {
		case <-this.Ctx.Done():
			return
		case ele := <-out:
			this.Log.Infof("把事件组%s发送到数据转换池", ele.Key)
			this.Next.Push(ele.Events)
		}
	}
}

func (this *Manager) Close() {
	this.Log.Info("关闭manager")
	this.sess.Close()
	if this.aggregator != nil {
		this.Log.Info("关闭聚合器逻辑开始")
		this.aggregator.Close()
	}
	this.cancel()
	this.wg.Wait()
	this.errHr.Close()
	if this.Next != nil {
		this.Next.Close()
	}
}

func (this *Manager) seeDelivery(info amqp.Delivery) {
	this.Log.Info("$$$$$$$$$$$$$获得的信息为:$$$$$$$$$$$$$")
	this.Log.Infof("Acknowledger: %v", info.Acknowledger)
	this.Log.Infof("Headers: %#v", info.Headers)
	this.Log.Info("ContentType: ", info.ContentType)
	this.Log.Info("Encoding: ", info.ContentEncoding)
	this.Log.Info("Exchange: ", info.Exchange)
	this.Log.Info("RoutingKey: ", info.RoutingKey)
	this.Log.Infof("DeliveryMode: %d", info.DeliveryMode)
	this.Log.Infof("Priority: %d", info.Priority)
	this.Log.Info("CorrealationId: ", info.CorrelationId)
	this.Log.Info("ReplyTo: ", info.ReplyTo)
	this.Log.Info("Expiration: ", info.Expiration)
	this.Log.Info("MessageId: ", info.MessageId)
	this.Log.Infof("Timestamp: %v", info.Timestamp)
	this.Log.Info("Type: ", info.Type)
	this.Log.Info("UserId: ", info.UserId)
	this.Log.Info("AppId: ", info.AppId)
	this.Log.Infof("DeliveryTag: %v", info.DeliveryTag)
	this.Log.Infof("Redelivered: %v", info.Redelivered)
}

func (this *Manager) Consume(in <-chan amqp.Delivery, ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case info := <-in:
			if len(info.Body) == 0 {
				continue
			}
			e := event.EventV2{}
			if this.Cfg.Compress {
				if err := e.Decompress(info.Body); err != nil {
					go this.errHr.Push(err)
					continue
				}
			} else {
				if err := e.FromJSON(info.Body); err != nil {
					this.Log.Info("出错痴痴痴痴猜猜猜")
					this.Log.Info(string(info.Body))
					go this.errHr.Push(err)
					continue
				}
			}
			info.Ack(false)
			this.Log.Infof("获取事件:%s\n", e)
			// 使用worker过滤事件
			if !this.worker.Analyze(e) {
				this.Log.Infof("不符合条件，忽略事件")
				continue
			}
			// 查看聚合
			if this.aggregator != nil {
				if key, err := this.aggregator.Add(e); err != nil {
					this.Log.Debugf("事件(%s)聚合出错: %s", e, err)
					go this.errHr.Push(err)
				} else {
					this.Log.Debugf("事件聚合到%s键中", key)
				}
			} else {
				this.Next.Push([]event.EventV2{e})
			}

		}
	}
}
