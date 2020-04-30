package config

import (
	"container/list"
	"github.com/always-waiting/cobra-canal/event"
	"time"
)

type CollectorV2 struct {
	Data        map[string][]event.Event
	SendChan    chan []event.Event
	slot        []*list.List
	ticker      *time.Ticker
	slotNum     int
	curPos      int
	addTaskChan chan task
	cleanChan   chan bool
	stopChan    chan bool
}

type task struct {
	key    string
	circle int
	data   event.Event
}

func makeCollectorV2(aggreCfg *AggreConfig) (ret *CollectorV2) {
	ret = new(CollectorV2)
	ret.Data = make(map[string][]event.Event)
	ret.SendChan = make(chan []event.Event)
	ret.addTaskChan = make(chan task)
	ret.cleanChan = make(chan bool)
	ret.stopChan = make(chan bool)
	ret.slotNum = aggreCfg.Time
	ret.slot = make([]*list.List, ret.slotNum)
	for i := 0; i < ret.slotNum; i++ {
		ret.slot[i] = list.New()
	}
	go ret.Start()
	return
}

func (c *CollectorV2) Start() {
	c.ticker = time.NewTicker(time.Second)
	go c.start()
}

func (c *CollectorV2) start() {
	for {
		select {
		case <-c.ticker.C:
			c.tickHandler()
		case task := <-c.addTaskChan:
			c.addEvent(&task)
		case <-c.cleanChan:
			for _, l := range c.slot {
				c.scanAndRunTask(l)
			}
		case <-c.stopChan:
			return
		}
	}
}

func (c *CollectorV2) Clean() {
	c.cleanChan <- true
}

func (c *CollectorV2) Stop() {
	c.cleanChan <- true
	c.stopChan <- true
	close(c.SendChan)
}

func (c *CollectorV2) AddEvent(key string, e event.Event) (err error) {
	c.addTaskChan <- task{key: key, data: e}
	return

}

func (c *CollectorV2) addEvent(t *task) {
	_, ok := c.Data[t.key]
	if ok {
		c.Data[t.key] = append(c.Data[t.key], t.data)
	} else {
		c.Data[t.key] = []event.Event{t.data}
	}
	c.slot[c.curPos].PushBack(&task{circle: 1, key: t.key})
}

func (c *CollectorV2) tickHandler() {
	l := c.slot[c.curPos]
	c.scanAndRunTask(l)
	if c.curPos == c.slotNum-1 {
		c.curPos = 0
	} else {
		c.curPos++
	}
}

func (c *CollectorV2) scanAndRunTask(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		if t.circle > 0 {
			t.circle--
			e = e.Next()
			continue
		}
		if es, ok := c.Data[t.key]; ok {
			c.SendChan <- es
		}
		next := e.Next()
		l.Remove(e)
		delete(c.Data, t.key)
		e = next
	}
	if len(c.Data) == 0 {
		c.Data = make(map[string][]event.Event)
	}
}
