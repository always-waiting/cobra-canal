package monitor

import (
	"fmt"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/siddontang/go-log/log"
	"sync"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	HEADER        = "##############%s##############"
	DONE          = "##############Done##############"
	EXCHANGE_NAME = "cobra_handler"
)

func CreateHandlerV2(c *Monitor) (h *HandlerV2, err error) {
	h = &HandlerV2{
		Log:       c.Log,
		errHr:     c.ErrHr,
		Monitor:   c,
		bufferNum: c.cfg.CobraCfg.GetBuffer(),
		buffer:    make([]event.EventV2, 0),
	}
	if err = h.InitFilterManager(); err != nil {
		return
	}
	return
}

type HandlerV2 struct {
	Log       *log.Logger
	errHr     *cobraErrors.ErrHandlerV2
	Monitor   *Monitor
	factories []*Factory
	bufferNum int
	buffer    []event.EventV2
	lock      sync.Mutex
}

func (this *HandlerV2) InitFilterManager() (err error) {
	this.factories = make([]*Factory, 0)
	rulesCfg := this.Monitor.RulesCfg()
	for _, rule := range rulesCfg {
		fm, err := CreateFactory(rule)
		if err != nil {
			return err
		}
		this.factories = append(this.factories, fm)
	}
	return
}

func (this *HandlerV2) Start() {
	for _, f := range this.factories {
		go f.Start()
	}
}

func (this *HandlerV2) Close() {
	wg := sync.WaitGroup{}
	wg.Add(len(this.factories))
	for _, f := range this.factories {
		go func(f *Factory) {
			defer wg.Done()
			f.Close()
		}(f)
	}
	wg.Wait()
}

func (this *HandlerV2) OnRow(e *canal.RowsEvent) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONROW)
	this.Log.Info(e.String())
	defer func() { this.Log.Info(DONE) }()
	if len(this.buffer) >= this.bufferNum {
		this.Flush()
	}
	cobraRowEvents, err := event.ParseOnRowV2(e)
	if err != nil {
		this.errHr.Push(err)
		return nil
	}
	this.Log.Debug("把合法事件推送到过滤队列")
	for _, event := range cobraRowEvents {
		if event.Err != nil {
			this.Log.Errorf("事件不合法%#v\n", event)
			this.errHr.Push(fmt.Sprintf("事件不合法: %#v\n", event))
		} else {
			this.buffer = append(this.buffer, event)
		}
	}
	this.Log.Debug("全部放入事件buffer池")
	return nil
}

func (this *HandlerV2) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONDDL)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnRotate(roateEvent *replication.RotateEvent) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONROTATE)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnTableChanged(schema string, table string) error {
	return nil
}
func (this *HandlerV2) OnXID(nextPos mysql.Position) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONXID)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnGTID(gtid mysql.GTIDSet) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONGTID)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnPosSynced(pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	if force {
		return nil
	}
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONPOSSYNCED)
	defer func() { this.Log.Info(DONE) }()
	this.Flush()
	return nil
}

func (this *HandlerV2) String() string {
	return "HandlerV2"
}

func (this *HandlerV2) Flush() {
	this.lock.Lock()
	defer func() { this.lock.Unlock() }()
	wg := sync.WaitGroup{}
	for _, e := range this.buffer {
		for _, fil := range this.factories {
			wg.Add(1)
			go func(f *Factory, eve event.EventV2) {
				defer func() { wg.Done() }()
				if f.Filter.IsTablePass(e) {
					if err := f.Filter.Push(e); err != nil {
						this.errHr.Push(err)
					}
				} else {
					id := f.Id()
					this.Log.Infof("工厂%d跳过事件: %s", id, e)
				}
			}(fil, e)
		}
		wg.Wait()
	}
	this.buffer = []event.EventV2{}
}
