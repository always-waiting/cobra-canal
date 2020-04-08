package transfer

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rabbitmq"
	"github.com/siddontang/go-log/log"
)

const (
	EXCHANGE_NAME = "cobra_transfer"
)

type Manager struct {
	Cfg  config.RuleConfigV2
	sess *rabbitmq.Session
	Log  *log.Logger
}

func CreateManager(rule config.RuleConfigV2) (ret *Manager, err error) {
	ret = &Manager{
		Cfg: rule,
	}
	if err = ret.SetLogger(); err != nil {
		return
	}
	if _, err = ret.SetSession(); err != nil {
		return
	}
	return
}

func (this *Manager) SetLogger() (err error) {
	logger, err := this.Cfg.GetLogger("transfermanager.log")
	this.Log = logger
	return
}

func (this *Manager) SetSession() (*rabbitmq.Session, error) {
	queueNames := this.Cfg.TransferWorkerName()
	sess, err := rabbitmq.New(EXCHANGE_NAME, this.Cfg.QueueAddr, queueNames...)
	sess.Log = this.Log
	this.sess = sess
	return sess, err
}

func (this *Manager) Push(es []event.EventV2) (err error) {
	var info []byte
	if this.Cfg.Compress {
		info, err = event.Compress(es)
	} else {
		info, err = event.ToJSON(es)
	}
	if err != nil {
		return
	}
	return this.sess.Push(info)
}

func (this *Manager) Close() {}
