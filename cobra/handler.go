package cobra

import (
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rules"
	"github.com/siddontang/go-log/log"
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	HEADER = "##############%s##############"
	DONE   = "##############Done##############"
)

type Handler struct {
	Rules     []*rules.Rule
	errHr     *cobraErrors.ErrHandler
	Log       *log.Logger
	lock      sync.Mutex
	buffer    []event.Event
	bufferNum int
	closed    bool
}

func CreateHandler(cfg []config.RuleConfig, buffer int) (h *Handler, err error) {
	h = new(Handler)
	h.Rules = make([]*rules.Rule, 0)
	h.buffer = make([]event.Event, 0, buffer)
	h.bufferNum = buffer
	if len(cfg) == 0 {
		cfg = append(cfg, config.RuleConfig{})
	}
	for _, ruleCfg := range cfg {
		var rule rules.Rule
		if rule, err = rules.CreateRule(ruleCfg); err != nil {
			return
		}
		h.Rules = append(h.Rules, &rule)
	}
	return
}

func (h *Handler) Start() {
	h.Log.Debug("开启所有规则处理器...")
	for _, rule := range h.Rules {
		go rule.Start()
	}
}

func (h *Handler) Stop() {
	h.lock.Lock()
	defer func() {
		h.closed = true
		h.lock.Unlock()
	}()
	h.Log.Debug("关闭规则处理器...")
	var wg sync.WaitGroup
	for _, rule := range h.Rules {
		wg.Add(1)
		go func(r *rules.Rule) {
			r.Close()
			wg.Done()
		}(rule)
	}
	wg.Wait()
	h.Log.Debug("所有规则处理器关闭")
}

func (h *Handler) OnRow(e *canal.RowsEvent) error {
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONROW)
	h.Log.Info(e.String())
	defer func() { h.Log.Info(DONE) }()
	if len(h.buffer) >= h.bufferNum {
		h.flush()
	}
	h.Log.Debug("转换数据格式")
	cobraRowEvents, err := event.ParseOnRow(e)
	if err != nil {
		h.errHr.Push(err)
		return nil
	}
	h.Log.Debug("把合法事件，放入事件池")
	for _, event := range cobraRowEvents {
		if event.Err != nil {
			h.Log.Errorf("事件不合法%#v\n", event)
			h.errHr.Push(errors.Errorf("事件不合法: %#v\n", event))
		} else {
			h.buffer = append(h.buffer, event)
		}
	}
	return nil
}

func (h *Handler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if h.closed {
		return nil
	}
	h.lock.Lock()
	defer func() { h.lock.Unlock() }()
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONDDL)
	h.Log.Info(string(queryEvent.Schema))
	h.Log.Info(string(queryEvent.Query))
	defer func() { h.Log.Info(DONE) }()
	h.Log.Debug("转换数据格式")
	cobraRowEvent, err := event.ParseOnDDL(queryEvent)
	if err != nil {
		h.errHr.Push(err)
		return nil
	}
	h.Log.Debug("判断生成事件是否合法")
	if cobraRowEvent.Err != nil {
		h.Log.Errorf("事件不合法%#v\n", cobraRowEvent)
		return nil
	}
	h.Log.Debug("对合法事件，在不同规则下进行不同处理")
	for _, rule := range h.Rules {
		rule.Push(cobraRowEvent)
	}
	return nil
}

func (h *Handler) OnRotate(roateEvent *replication.RotateEvent) error {
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONROTATE)
	defer func() { h.Log.Info(DONE) }()
	return nil
}

func (h *Handler) OnTableChanged(schema string, table string) error {
	return nil
}
func (h *Handler) OnXID(nextPos mysql.Position) error {
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONXID)
	defer func() { h.Log.Info(DONE) }()
	return nil
}

func (h *Handler) OnGTID(gtid mysql.GTIDSet) error {
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONGTID)
	defer func() { h.Log.Info(DONE) }()
	return nil
}

func (h *Handler) OnPosSynced(pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	if force {
		return nil
	}
	h.Log.Infof(HEADER, event.SYNC_TYPE_ONPOSSYNCED)
	defer func() { h.Log.Info(DONE) }()
	h.flush()
	return nil
}

func (h *Handler) String() string {
	return "Handler"
}

func (h *Handler) flush() {
	h.lock.Lock()
	defer func() { h.lock.Unlock() }()
	if h.closed {
		return
	}
	for _, e := range h.buffer {
		for _, rule := range h.Rules {
			rule.Push(e)
		}
	}
	h.buffer = []event.Event{}
}
