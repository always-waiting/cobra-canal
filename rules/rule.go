package rules

import (
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

const (
	LOAD_ERR1 = "规则名为空"
	LOAD_ERR2 = "规则(%s)未注册"
	LOAD_ERR3 = "生成%s规则失败"
)

type Rule struct {
	ruler        Ruler                   `description:"规则"`
	eventChannel chan event.Event        `description:"事件队列"`
	errHr        *cobraErrors.ErrHandler `description:"错误处理对象"`
	isRulerClose chan bool
	closed       bool
	isReady      bool
	Log          *log.Logger
}

var ruleMakers = map[string]func(config.RuleConfig) (Ruler, error){
	"fake": func(cfg config.RuleConfig) (Ruler, error) {
		r := &BasicRuler{}
		r.SetName("fake")
		r.AddFilterFunc(func(e *event.Event) (bool, error) {
			return true, nil
		})
		return r, nil
	},
}

func RegisterRuleMaker(name string, f func(config.RuleConfig) (Ruler, error)) {
	ruleMakers[name] = f
}

func CreateRule(cfg config.RuleConfig) (rule Rule, err error) {
	if cfg.Name == "" {
		err = errors.New(LOAD_ERR1)
		return
	}
	if rule, err = InitRule(cfg); err != nil {
		return
	}
	var ruler Ruler
	if cfg.Name == "" {
		cfg.Name = "fake"
	}
	f, ok := ruleMakers[cfg.Name]
	if !ok {
		err = errors.Errorf(LOAD_ERR2, cfg.Name)
	}
	ruler, err = f(cfg)
	if ruler == nil {
		err = errors.Errorf(LOAD_ERR3, cfg.Name)
		return
	}
	if err = ruler.LoadConfig(cfg); err != nil {
		return
	}
	ruler.SetLogger(rule.Log)
	rule.SetRuler(ruler)
	return
}

func InitRule(cfg config.RuleConfig) (rule Rule, err error) {
	rule = Rule{}
	rule.eventChannel = make(chan event.Event, cfg.GetBufferNum())
	rule.errHr = cobraErrors.MakeErrHandler(cfg.ErrSenderCfg.Parse(), cfg.GetBufferNum())
	rule.isRulerClose = make(chan bool, 1)
	rule.Log, err = cfg.LogCfg.GetLogger()
	return
}

func (this *Rule) SetRuler(r Ruler) {
	this.ruler = r
}

func (this *Rule) Push(e event.Event) {
	if this.closed {
		this.Log.Errorf("%s规则事件池已经关闭，不能放入事件", this.ruler.GetName())
		return
	}
	this.eventChannel <- e
}

func (this *Rule) Close() error {
	if this.closed {
		return nil
	}
	close(this.eventChannel)
	this.closed = true
	<-this.isRulerClose
	err := this.ruler.Close()
	this.errHr.Close()
	this.Log.Infof("%s规则的错误处理器关闭", this.ruler.GetName())
	return err
}

func (this *Rule) Start() {
	if this.isReady {
		return
	}
	this.Log.Infof("%s规则的事件池开启...", this.ruler.GetName())
	this.isReady = true
	go this.errHr.Send()
	this.Log.Infof("%s规则的错误处理器开启", this.ruler.GetName())
	this.ruler.Start()
	for {
		e, isOpen := <-this.eventChannel
		if !isOpen {
			break
		}
		this.Log.Debugf("%s规则发现有事件需要处理:\n%s", this.ruler.GetName(), e.String())
		if err := this.ruler.HandleEvent(e); err != nil {
			go this.errHr.Push(err)
		}
		this.Log.Debug("处理完毕")
	}
	this.Log.Infof("%s规则的事件池关闭", this.ruler.GetName())
	this.isRulerClose <- true
}
