package rules

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/consumer"
	"github.com/always-waiting/cobra-canal/event"
	"sync"

	"github.com/jinzhu/gorm"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/client"
)

const (
	AGGREGATOR_HEADER = "-------->缓存键值为:%s<--------"
)

type BasicRuler struct {
	name             string
	aggregator       config.Aggregatable
	consumers        map[string]*consumer.Consume
	MasterDB         *gorm.DB
	DBClient         *client.Conn
	filter           FilterHandler
	isReady          bool
	closed           bool
	closeAggregation chan bool
	Log              *log.Logger
	transferFunc     map[string]func([]event.Event) (interface{}, error)
	hasLoadConfig    bool
}

func (this *BasicRuler) SetLogger(l *log.Logger) {
	this.Log = l
}

func (this *BasicRuler) LoadConfig(ruleCfg config.RuleConfig) (err error) {
	if this.hasLoadConfig {
		return
	}
	this.hasLoadConfig = true
	this.aggregator = ruleCfg.InitAggregator()
	this.closeAggregation = make(chan bool, 1)
	if len(ruleCfg.ReplySync) != 0 {
		this.filter.LoadReplySyncFilter(ruleCfg.ReplySync)
	}
	if ruleCfg.HasTableFilter() {
		this.filter.LoadTableFilter(ruleCfg.TableFilterCfg)
	}
	if len(ruleCfg.ConsumerCfg) == 0 {
		this.Log.Info("构建fake消费器......")
		ruleCfg.ConsumerCfg = append(ruleCfg.ConsumerCfg, &config.ConsumerConfig{})
	}
	this.consumers = make(map[string]*consumer.Consume)
	for _, consumerCfg := range ruleCfg.ConsumerCfg {
		if consume, err := consumer.CreateConsume(consumerCfg); err != nil {
			return err
		} else {
			if f, ok := this.transferFunc[consume.GetName()]; ok {
				consume.SetTransferFunc(f)
			}
			this.consumers[consume.GetName()] = &consume
		}
	}
	if ruleCfg.MasterDBCfg != nil {
		var gormAddr string
		if gormAddr, err = ruleCfg.MasterDBCfg.ToGormAddr(); err != nil {
			return
		}
		if this.MasterDB, err = gorm.Open("mysql", gormAddr); err != nil {
			return
		}
		if this.DBClient, err = client.Connect(
			ruleCfg.MasterDBCfg.Addr,
			ruleCfg.MasterDBCfg.User,
			ruleCfg.MasterDBCfg.Passwd,
			ruleCfg.MasterDBCfg.Db,
		); err != nil {
			return
		}
	}
	return
}

func (this *BasicRuler) AddTransferFunc(name string, f func([]event.Event) (interface{}, error)) {
	if this.transferFunc == nil {
		this.transferFunc = make(map[string]func([]event.Event) (interface{}, error))
	}
	this.transferFunc[name] = f
}

/*
func (this *BasicRuler) SetConsumerTransferFunc(name string, f func([]event.Event) (interface{}, error)) (err error) {
	if _, ok := this.consumers[name]; !ok {
		err = errors.Errorf("未初始化的消费器%s", name)
		return
	}
	this.consumers[name].SetTransferFunc(f)
	return err
}
*/

func (this *BasicRuler) Start() {
	this.StartConsume()
	if this.IsAggre() {
		go this.StartAggregation()
	}
}

func (this *BasicRuler) Close() (err error) {
	if this.IsAggre() {
		err = this.CloseAggregation()
	}
	this.CloseConsume()
	this.DBClient.Close()
	this.MasterDB.Close()
	return
}

func (this *BasicRuler) HandleEvent(e event.Event) (err error) {
	// 应用过滤规则
	flag, err := this.Filter(&e)
	if err != nil || !flag {
		err = this.ModifyErr(err)
		this.Log.Debug("事件跳过")
		return
	}
	// 消费事件
	if this.IsAggre() {
		err = this.Aggregate(e)
	} else {
		this.Push([]event.Event{e})
	}
	return
}

func (this *BasicRuler) Debug() {}
func (this *BasicRuler) Info()  {}

func (this *BasicRuler) AddFilterFunc(f func(*event.Event) (bool, error)) {
	this.filter.AddFilterFunc(f)
}

func (this *BasicRuler) SetName(name string) {
	this.name = name
}

func (this *BasicRuler) GetName() string {
	return this.name
}

func (this *BasicRuler) Filter(e *event.Event) (bool, error) {
	this.Log.Debug("根据规则进行过滤")
	return this.filter.Filter(e)
}

func (this *BasicRuler) IsAggre() bool {
	return this.aggregator != nil
}

func (this *BasicRuler) GetAggreRule(name string) *config.IdxRuleConfig {
	return this.aggregator.GetRule(name)
}

func (this *BasicRuler) DiffData(r *config.IdxRuleConfig, d1, d2 map[string]interface{}) (map[string]interface{}, error) {
	return this.aggregator.DiffData(r, d1, d2)
}

func (this *BasicRuler) GetAggreKey(e event.Event) (string, error) {
	return this.aggregator.GetIdxValue(e)
}

func (this *BasicRuler) StartConsume() {
	if this.isReady {
		return
	}
	this.isReady = true
	for _, consumer := range this.consumers {
		go consumer.Start()
	}
}

func (this *BasicRuler) CloseConsume() {
	if this.closed {
		return
	}
	this.closed = true
	var wg sync.WaitGroup
	for _, consume := range this.consumers {
		wg.Add(1)
		go func(c *consumer.Consume) {
			c.Close()
			wg.Done()
		}(consume)
	}
	wg.Wait()
}

func (this *BasicRuler) Push(events []event.Event) {
	var wg sync.WaitGroup
	for _, consume := range this.consumers {
		wg.Add(1)
		go func(c *consumer.Consume) {
			this.Log.Debugf("%s规则向%s消费池推送事件包", this.name, c.GetName())
			c.Push(events)
			wg.Done()
		}(consume)
	}
	wg.Wait()
}

func (this *BasicRuler) StartAggregation() {
	this.Log.Info("聚合器开启...")
	sendKey := this.aggregator.GetSendChan()
	for {
		key, isOpen := <-sendKey
		if !isOpen {
			break
		}
		events, err := this.aggregator.MoveEvents(key)
		if err != nil {
			continue
		}
		this.Log.Debugf("聚合器消费%s键的事件包", key)
		this.Push(events)
	}
	this.closeAggregation <- true
	this.Log.Info("聚合器关闭")
}

func (this *BasicRuler) CloseAggregation() error {
	this.aggregator.Stop()
	<-this.closeAggregation
	return nil
}

func (this *BasicRuler) Aggregate(e event.Event) (err error) {
	key, err := this.aggregator.GetIdxValue(e)
	if err != nil {
		return
	}
	if key != "" {
		this.Log.Debugf(AGGREGATOR_HEADER, key)
		if this.aggregator.HasIdx(key) {
			err = this.aggregator.AppendEvent(key, e)
		} else {
			err = this.aggregator.CreateEvent(key, e)
		}
	}
	return
}

func (this *BasicRuler) ModifyErr(err error) (ret error) {
	if err != nil {
		str := fmt.Sprintf("规则%s报错: %s", this.GetName(), err.Error())
		ret = errors.New(str)
	}
	return
}
