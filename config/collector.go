package config

import (
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"sync"
	"time"
)

const (
	COLLECTOR_ERR1 = "键%s不存在"
)

type Collector struct {
	Mutex    sync.Mutex               `description:"防止数据竞争的锁"`
	Data     map[string][]event.Event `description:"存储的数据"`
	TList    map[string]*time.Timer   `description:"具体的延时对象"`
	SendChan chan string              `description:"发送管道，到时间会把键发送到这里"`
	Interval time.Duration            `description:"缓存时间"`
}

func makeCollector(aggreCfg *AggreConfig) (collector *Collector) {
	collector = new(Collector)
	collector.Data = make(map[string][]event.Event)
	collector.TList = make(map[string]*time.Timer)
	collector.SendChan = make(chan string)
	collector.Interval = time.Second * time.Duration(aggreCfg.Time)
	return
}

func (c *Collector) Lock() {
	c.Mutex.Lock()
}

func (c *Collector) Unlock() {
	c.Mutex.Unlock()
}

func (c *Collector) hasIdx(key string) bool {
	var ret bool
	c.Lock()
	if _, ok := c.Data[key]; ok {
		ret = true
	}
	c.Unlock()
	return ret
}

func (c *Collector) IsEmpty() bool {
	var ret bool
	c.Lock()
	if len(c.Data) == 0 {
		ret = true
	}
	c.Unlock()
	return ret
}

/*
把所有定时器都设置成1s,以这种方式情况
如果直接处理，错误无法发送出去
*/
func (c *Collector) Clean() {
	c.Lock()
	for key, timer := range c.TList {
		sendKey := key
		if timer.Stop() {
			c.TList[sendKey] = time.AfterFunc(1*time.Second, func() {
				c.SendChan <- sendKey
			})
		}
	}
	c.Unlock()
}

func (c *Collector) AppendEvent(key string, e event.Event) (err error) {
	defer c.RecoverErr(&err)
	c.Lock()
	c.Data[key] = append(c.Data[key], e)
	if c.TList[key].Stop() {
		c.TList[key] = time.AfterFunc(c.Interval, func() {
			c.SendChan <- key
		})
	}
	c.Unlock()
	return
}

func (c *Collector) CreateEvent(key string, e event.Event) (err error) {
	defer c.RecoverErr(&err)
	c.Lock()
	c.Data[key] = []event.Event{e}
	c.TList[key] = time.AfterFunc(c.Interval, func() {
		c.SendChan <- key
	})
	c.Unlock()
	return
}

func (c *Collector) RecoverErr(err *error) {
	if e := recover(); e != nil {
		switch x := e.(type) {
		case error:
			*err = x
		case string:
			*err = errors.New(x)
		default:
			*err = errors.Errorf(ERROR2, e)
		}
	}
}

func (c *Collector) MoveEvents(key string) (events []event.Event, err error) {
	c.Lock()
	defer c.Unlock()
	events, ok := c.Data[key]
	if !ok {
		err = errors.Errorf(COLLECTOR_ERR1, key)
		return
	}
	delete(c.Data, key)
	delete(c.TList, key)
	return
}
