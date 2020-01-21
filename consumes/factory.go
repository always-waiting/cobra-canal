package consumes

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/MakeNowJust/heredoc"
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

const (
	HEADER     = ">>>>>>>>%s<<<<<<<<\n"
	DONE       = "<<<<<<<<%s>>>>>>>>\n"
	EVENT_LINE = "################"
	LOAD_ERR1  = "消费类型(%s)未定义"
	LOAD_ERR2  = "生成%s消费下游失败"
)

var consumerMakers = map[string]func(*config.ConsumerConfig) (Consumer, error){
	"fake": func(cfg *config.ConsumerConfig) (Consumer, error) {
		cr := new(BaseConsumer)
		cr.SetName("fake")
		return cr, nil
	},
}

func RegisterConsumerMaker(name string, f func(*config.ConsumerConfig) (Consumer, error)) {
	consumerMakers[name] = f
}

func CreateConsume(cfg *config.ConsumerConfig) (ret Factory, err error) {
	if ret, err = InitConsume(cfg); err != nil {
		return
	}
	var csr Consumer
	if cfg.Type == "" {
		cfg.Type = "fake"
	}
	f, ok := consumerMakers[cfg.Type]
	if !ok {
		err = errors.Errorf(LOAD_ERR1, cfg.Type)
	}
	for i := 0; i < ret.consumerNum; i++ {
		if csr, err = f(cfg); err != nil {
			return
		}
		if csr == nil {
			err = errors.Errorf(LOAD_ERR2, cfg.Type)
			return
		}
		csr.SetLogger(ret.Log)
		csr.SetNumber(i)
		err = csr.Open()
		if err != nil {
			return
		}
		ret.SetConsumer(csr)
	}
	return
}

type Factory struct {
	consumer        []Consumer
	eventsChan      chan []event.Event
	closed          bool
	isReady         bool
	isConsumerClose chan bool
	errHr           *cobraErrors.ErrHandler
	Log             *log.Logger
	consumerNum     int
	rulerNum        int
}

func MakeFakeConsume() Factory {
	cfg := config.ConsumerConfig{}
	c, _ := InitConsume(&cfg)
	cr := new(BaseConsumer)
	cr.SetName("fake")
	cr.SetTransferFunc(func(e []event.Event) (out interface{}, err error) { return })
	cr.Log = c.Log
	c.SetConsumer(cr)
	return c
}

func InitConsume(cfg *config.ConsumerConfig) (Factory, error) {
	var err error
	consume := Factory{}
	consume.eventsChan = make(chan []event.Event, cfg.GetBufferNum())
	consume.isConsumerClose = make(chan bool, 1)
	consume.errHr = cobraErrors.MakeErrHandler(cfg.ErrSenderCfg.Parse(), cfg.GetBufferNum())
	consume.Log, err = cfg.LogCfg.GetLogger()
	consume.consumerNum = cfg.Worker()
	consume.consumer = make([]Consumer, 0)
	return consume, err
}

func (this *Factory) SetRulerNum(i int) {
	this.rulerNum = i
	if this.consumer != nil {
		for _, csr := range this.consumer {
			csr.SetRuleNum(i)
		}
	}
}

func (this *Factory) GetRuleNum() int {
	return this.rulerNum
}

func (this *Factory) SetTransferFunc(f func([]event.Event) (interface{}, error)) {
	for _, csr := range this.consumer {
		csr.SetTransferFunc(f)
	}
}

func (this *Factory) SetConsumer(consumer Consumer) {
	this.consumer = append(this.consumer, consumer)
}

func (this *Factory) GetName() string {
	if len(this.consumer) != 0 {
		return this.consumer[0].GetName()
	}
	return ""
}

func (this *Factory) Push(input []event.Event) {
	if this.closed {
		this.Log.Errorf("Rule%d: %s消费池已经关闭，无法放入事件包", this.GetRuleNum(), this.GetName())
		return
	}
	this.eventsChan <- input
}

func (this *Factory) Close() error {
	if this.closed {
		return nil
	}
	close(this.eventsChan)
	this.closed = true
	<-this.isConsumerClose
	var err error
	for _, csr := range this.consumer {
		err = csr.Close()
		if err != nil {
			this.Log.Error(err)
		}
		base := reflect.ValueOf(csr).Elem().FieldByName("BaseConsumer")
		if base.IsValid() && base.CanAddr() {
			f := base.Addr().MethodByName("Close")
			if f.IsValid() {
				f.Call(nil)
			} else {
				this.Log.Error("没有找到Close函数")
			}
		} else {
			this.Log.Error("没有找到BaseConsumer对象")
		}
	}
	//err := this.consumer.Close()
	this.Log.Infof("Rule%d: %s消费器关闭", this.GetRuleNum(), this.GetName())
	this.errHr.Close()
	this.Log.Infof("Rule%d: %s消费错误处理器关闭", this.GetRuleNum(), this.GetName())
	return err
}

func (this *Factory) Start() {
	this.Log.Infof("Rule%d: %s消费池开启...", this.GetRuleNum(), this.GetName())
	if this.isReady {
		return
	}
	this.isReady = true
	go this.errHr.Send()
	var wg sync.WaitGroup
	for _, csr := range this.consumer {
		wg.Add(1)
		go func(c Consumer) {
			num := c.Number()
			for {
				input, isOpen := <-this.eventsChan
				if !isOpen {
					break
				}
				this.Log.Debugf("Rule%d-Csr%d: %s", this.rulerNum, num, fmt.Sprintf(HEADER, "消费开始"))
				this.Log.Debugf("Rule%d-Csr%d: 发现如下事件包:", this.rulerNum, num)
				for _, e := range input {
					this.Log.Debugf("Rule%d-Csr%d: %s", this.rulerNum, num, EVENT_LINE)
					this.Log.Debugf("Rule%d-Csr%d: %s", this.rulerNum, num, e.String())
				}
				this.Log.Debugf("Rule%d-Csr%d: 消费器%s转换事件包", this.rulerNum, num, c.GetName())
				data, err := c.Transfer(input)
				if err != nil {
					go this.errHr.Push(this.modifyErr(err, input))
					continue
				}
				this.Log.Debugf("Rule%d-Csr%d: 转换后的信息为:%#v\n", this.rulerNum, num, data)
				this.Log.Debugf("Rule%d-Csr%d: 消费器%s消费事件包", this.rulerNum, num, c.GetName())
				if err = c.Solve(data); err != nil {
					go this.errHr.Push(this.modifyErr(err, input))
				}
				this.Log.Debugf("Rule%d-Csr%d: 消费完毕", this.rulerNum, num)
			}
			wg.Done()
		}(csr)
	}
	wg.Wait()
	this.Log.Infof("Rule%d: %s消费池关闭", this.GetRuleNum(), this.GetName())
	this.isConsumerClose <- true
}

func (this *Factory) modifyErr(err error, input []event.Event) (retErr error) {
	if err == nil {
		return
	}
	docTmp := heredoc.Doc(`<PRE>
	事件包为:<br>
	%s<br>
	消费器%s错误信息:<br>
	%s<br>
	</PRE>`)
	inputStr := make([]string, 0)
	for _, data := range input {
		inputStr = append(inputStr, data.String())
	}
	retErr = errors.Errorf(docTmp, strings.Join(inputStr, "<br>###########<br>"), this.GetName(), err.Error())
	return
}

func (this *Factory) Reset() error {
	var err error
	this.eventsChan = make(chan []event.Event, cap(this.eventsChan))
	if this.errHr != nil {
		this.errHr.Reset()
	}
	for _, csr := range this.consumer {
		if err = csr.Reset(); err != nil {
			break
		}
		base := reflect.ValueOf(csr).Elem().FieldByName("BaseConsumer")
		if base.IsValid() && base.CanAddr() {
			f := base.Addr().MethodByName("Reset")
			if f.IsValid() {
				f.Call(nil)
			} else {
				this.Log.Error("没有找到Reset函数")
			}
		} else {
			this.Log.Error("没有找到BaseConsumer对象")
		}
	}
	if err != nil {
		return err
	}
	this.isReady = false
	this.closed = false
	return nil
}

func (this *Factory) ConsumerNum() int {
	return this.consumerNum
}

func (this *Factory) ActiveConsumerNum() int {
	var num int
	for _, csr := range this.consumer {
		if !csr.IsClosed() {
			num = num + 1
		}
	}
	return num
}

func (this *Factory) PoolCap() int {
	return cap(this.eventsChan)
}

func (this *Factory) PoolLen() int {
	return len(this.eventsChan)
}

func (this *Factory) IsClosed() bool {
	return this.closed
}
