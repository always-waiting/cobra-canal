package collection

import (
	"context"
	"sync"
	"time"

	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
)

var (
	ErrCfgEmpty = errors.New("配置为空")
)

type Aggregatable interface{}

type Indexer interface {
	Idx(event.EventV2) (string, error)
}

type Element struct {
	Key    string
	Events []event.EventV2
}

type Aggregator struct {
	gatherMap map[string]Indexer `description:"存储每个表解析的方式"`
	Mutex     sync.Mutex         `description:"防止数据竞争的锁"`
	Interval  time.Duration      `description:"缓存时间"`
	keyChan   chan string        `description:"键值管道，到时间会把键发送到这个管道"`
	pool      map[string]Element `description:"聚合池"`
	timerList map[string]*time.Timer
	Ctx       context.Context
	cancel    context.CancelFunc
	ready     bool
}

func CreateAggregator(cfg *AggreConfig) (*Aggregator, error) {
	if cfg == nil {
		return nil, ErrCfgEmpty
	}
	gatherMap := make(map[string]Indexer)
	for _, idx := range cfg.IdxRulesCfg {
		tables := idx.Tables
		for _, table := range tables {
			gatherMap[table] = idx
		}
	}
	du, _ := time.ParseDuration(fmt.Sprintf("%ds", cfg.Time))
	ctx, cancel := context.WithCancel(context.Background())
	ag := &Aggregator{
		gatherMap: gatherMap,
		Interval:  du,
		keyChan:   make(chan string, 0),
		pool:      make(map[string]Element),
		timerList: make(map[string]*time.Timer),
		Ctx:       ctx,
		cancel:    cancel,
	}
	return ag, nil
}

func (this *Aggregator) Clean() {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	this.pool = make(map[string]Element)
	this.timerList = make(map[string]*time.Timer)
}

func (this *Aggregator) Flush() {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	for key, timer := range this.timerList {
		if timer.Stop() {
			go func(sendKey string) {
				//fmt.Printf("发送%s键值\n", sendKey)
				this.keyChan <- sendKey
			}(key)
		}
	}
}

func (this *Aggregator) Close() {
	this.ready = false
	this.Flush()
	for !this.IsEmpty() {
		time.Sleep(time.Second)
	}
	this.cancel()
}

func (this *Aggregator) Collection() (out chan Element) {
	this.ready = true
	out = make(chan Element, 0)
	go func() {
		for {
			select {
			case key := <-this.keyChan:
				this.MoveTo(key, out)
			case <-this.Ctx.Done():
				return
			}
		}
	}()
	return
}

func (this *Aggregator) IsEmpty() bool {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	return len(this.pool) == 0
}

func (this *Aggregator) Add(e event.EventV2) (key string, err error) {
	table := e.Table.Name
	indexer, ok := this.gatherMap[table]
	if !ok {
		err = errors.Errorf("没有%s表聚合规则", table)
		return
	}
	key, err = indexer.Idx(e)
	if err != nil {
		return
	}
	if this.HasIdx(key) {
		err = this.Push(key, e)
	} else {
		err = this.Create(key, e)
	}
	return
}

func (this *Aggregator) HasIdx(key string) bool {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	_, ok := this.pool[key]
	return ok
}

func (this *Aggregator) Create(key string, e event.EventV2) (err error) {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	if !this.ready {
		err = errors.New("聚合器已经启动关闭逻辑，不再接收事件")
		return
	}
	ele := Element{
		Key:    key,
		Events: []event.EventV2{e},
	}
	this.timerList[key] = time.AfterFunc(this.Interval, func() {
		this.keyChan <- ele.Key
	})
	this.pool[key] = ele
	return
}

func (this *Aggregator) Push(key string, e event.EventV2) (err error) {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	if !this.ready {
		err = errors.New("聚合器已经启动关闭逻辑，不再接收事件")
		return
	}
	if !this.timerList[key].Stop() { // timer已经调用,或者stop了
		this.Mutex.Unlock()
		for this.HasIdx(key) {
		}
		this.Mutex.Lock()
		ele := Element{
			Key:    key,
			Events: []event.EventV2{e},
		}
		this.timerList[key] = time.AfterFunc(this.Interval, func() {
			this.keyChan <- ele.Key
		})
		this.pool[key] = ele
	} else {
		ele, _ := this.pool[key]
		ele.Events = append(ele.Events, e)
		this.pool[key] = ele
		this.timerList[key].Reset(this.Interval)
	}
	return
}

func (this *Aggregator) MoveTo(key string, out chan<- Element) (ret Element, err error) {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	ret, ok := this.pool[key]
	if !ok {
		err = errors.Errorf("键%s不存在", key)
		return
	}
	delete(this.pool, key)
	delete(this.timerList, key)
	out <- ret
	return
}

func (this *Aggregator) String() string {
	return fmt.Sprintf("gggg")
}
