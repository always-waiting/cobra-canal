package rules

import (
	"context"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/siddontang/go-log/log"
	"github.com/streadway/amqp"
	"sync"
)

var (
	errQueueNotDefined = errors.New("没有定义队列信息")
	ErrOutOfIndex      = errors.New("下标越界")
)

type Manager struct {
	Wg       sync.WaitGroup
	Cfg      config.RuleConfigV2
	sess     *Session
	Log      *log.Logger
	Next     *Manager
	workType config.WorkerType
	errHr    *errors.ErrHandlerV2
	Ctx      context.Context
	cancel   context.CancelFunc
}

func CreateManagerWithSess(rule config.RuleConfigV2, wt config.WorkerType, sess *Session) (ret *Manager, err error) {
	ret = &Manager{Cfg: rule, workType: wt}
	ret.Ctx, ret.cancel = context.WithCancel(context.Background())
	if err = ret.SetLogger(); err != nil {
		return
	}
	if err = ret.SetSession(sess); err != nil {
		return
	}
	if err = ret.SetErrHr(); err != nil {
		return
	}
	return
}

func CreateManager(rule config.RuleConfigV2, wt config.WorkerType) (ret *Manager, err error) {
	ret = &Manager{Cfg: rule, workType: wt}
	ret.Ctx, ret.cancel = context.WithCancel(context.Background())
	if err = ret.SetLogger(); err != nil {
		return
	}
	if err = ret.SetSession(nil); err != nil {
		return
	}
	if err = ret.SetErrHr(); err != nil {
		return
	}
	return
}

func (this *Manager) GetSession() *Session {
	return this.sess
}

func (this *Manager) QueueNames() []string {
	return this.sess.QueueNames()
}

func (this *Manager) Push(in interface{}, ids ...int) (err error) {
	return this.sess.Push(in, ids...)
}

func (this *Manager) PushByIdx(idx int, in interface{}) (err error) {
	return this.sess.PushByIdx(idx, in)
}

func (this *Manager) SetLogger() (err error) {
	logger, err := this.Cfg.GetLogger(this.workType)
	this.Log = logger
	return
}

/*
func (this *Manager) SetSession() (*rabbitmq.Session, error) {
	names, err := this.Cfg.WorkersName(this.workType)
	if err != nil {
		return nil, err
	}
	sess, err := rabbitmq.New(this.workType.ExchangeName(), this.Cfg.QueueAddr, names...)
	sess.Log = this.Log
	this.sess = sess
	return sess, err
}
*/

func (this *Manager) SetSession(sess *Session) error {
	if sess != nil {
		this.sess = sess
		return nil
	}
	names, err := this.Cfg.WorkersName(this.workType)
	if err != nil {
		return err
	}
	buf, err := this.Cfg.BuffNum(this.workType)
	if err != nil {
		return err
	}
	sess = NewSession(buf, names...)
	this.sess = sess
	return nil
}

func (this *Manager) Id() (int64, error) {
	return int64(this.Cfg.Id), nil
}

func (this *Manager) Name() (string, error) {
	return this.Cfg.ManagerName(this.workType)
}

func (this *Manager) IsTablePass(e event.EventV2) bool {
	if this.Cfg.HasTableFilter() {
		tafilter := this.Cfg.TableFilter()
		return tafilter.IsTablePass(e.Table.Schema, e.Table.Name)
	}
	return true
}

func (this *Manager) SetErrHr() (err error) {
	eHr := this.Cfg.ErrHandler()
	this.errHr = &eHr
	return
}

func (this *Manager) SetNextManager(wt config.WorkerType) (err error) {
	this.Next, err = CreateManager(this.Cfg, wt)
	return
}

func (this *Manager) ErrSend() {
	this.errHr.Send()
}

func (this *Manager) ErrPush(i interface{}) {
	go this.errHr.Push(i)
}

/*
func (this *Manager) StreamAll() ([]<-chan amqp.Delivery, error) {
	return this.sess.StreamAll()
}
*/

func (this *Manager) StreamAll() ([]chan interface{}, error) {
	return this.sess.StreamAll()
}

/*
func (this *Manager) ReConnSignal() <-chan bool {
	ch := this.sess.ReChanSignal
	return (<-chan bool)(ch)
}
*/

func (this *Manager) SessClose() {
	this.sess.Close()
}

func (this *Manager) Close() {
	this.cancel()
	this.Wg.Wait()
	this.errHr.Close()
	if this.Next != nil {
		this.Next.Close()
	}
}

func (this *Manager) SeeDelivery(info amqp.Delivery) {
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

/*
func (this *Manager) Ack(tag uint64, multiple bool) error {
	return this.sess.Chan().Ack(tag, multiple)
}
*/
