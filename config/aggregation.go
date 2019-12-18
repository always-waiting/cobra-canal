package config

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

type Aggregatable interface {
	GetRule(string) *IdxRuleConfig
	GetIdxValue(event.Event) (string, error)
	HasIdx(string) bool
	AppendEvent(string, event.Event) error
	CreateEvent(string, event.Event) error
	GetSendChan() chan string
	MoveEvents(string) ([]event.Event, error)
	DiffData(*IdxRuleConfig, map[string]interface{}, map[string]interface{}) (map[string]interface{}, error)
	Stop()
	Reset()
}

func makeDefaultAggregator(r *RuleConfig) *Aggregator {
	ret := &Aggregator{}
	ret.CIdxGenerator = ret.defaultCIdxGenerator
	ret.SetRule(r.AggreCfg.IdxRulesCfg)
	ret.Collector = makeCollector(r.AggreCfg)
	return ret
}

type Aggregator struct {
	cfgMap        map[string]IdxRuleConfig
	CIdxGenerator func(event.Event) (string, error)
	Collector     *Collector
}

func (b *Aggregator) Stop() {
	b.Collector.Clean()
	for {
		if b.Collector.IsEmpty() {
			close(b.Collector.SendChan)
			break
		}
	}
}

func (b *Aggregator) Reset() {
	b.Collector.SendChan = make(chan string)
}

func (this *Aggregator) AppendEvent(key string, e event.Event) error {
	return this.Collector.AppendEvent(key, e)
}

func (this *Aggregator) CreateEvent(key string, e event.Event) error {
	return this.Collector.CreateEvent(key, e)
}

func (this *Aggregator) MoveEvents(key string) ([]event.Event, error) {
	return this.Collector.MoveEvents(key)
}

func (this *Aggregator) HasIdx(key string) bool {
	return this.Collector.hasIdx(key)
}

func (this *Aggregator) GetSendChan() chan string {
	return this.Collector.SendChan
}

func (this *Aggregator) GetIdxValue(e event.Event) (string, error) {
	return this.CIdxGenerator(e)
}

func (this *Aggregator) defaultCIdxGenerator(e event.Event) (retStr string, err error) {
	idxRule := this.GetRule(e.Table.Name)
	if idxRule != nil {
		if retStr, err = this.defaultIdxRuleParser(idxRule, e); err != nil {
			return
		}
		log.Debugf("缓存idx为:%s", retStr)
	} else {
		err = errors.Errorf("%s表的聚合规则没有定义", e.Table.Name)
	}
	return
}

func (this *Aggregator) defaultIdxRuleParser(idxR *IdxRuleConfig, e event.Event) (ret string, err error) {
	var idxField, actionField interface{}
	if idxField, err = e.GetColumnValue(0, idxR.IdxField); err != nil {
		return
	}
	ret = fmt.Sprintf("%v", idxField)
	if idxR.IdxPrefix != "" {
		switch idxR.IdxPrefix {
		case "TABLENAME":
			ret = fmt.Sprintf("%s:%s", e.Table.Name, ret)
		default:
			ret = fmt.Sprintf("%s:%s", idxR.IdxPrefix, ret)
		}
	}
	if idxR.AggreField != "" {
		switch e.Action {
		case "insert", "delete":
			if actionField, err = e.GetColumnValue(0, idxR.AggreField); err != nil {
				return
			}
		case "update":
			if actionField, err = e.GetColumnValue(1, idxR.AggreField); err != nil {
				return
			}
		}
		if actionField != nil {
			ret = fmt.Sprintf("%v%s%s", actionField, IDXRULE_SEPARATOR, ret)
		} else {
			err = errors.Errorf(IDXRULE_ERR1, idxR.AggreField)
		}
	}
	return
}

func (this *Aggregator) GetRule(table string) *IdxRuleConfig {
	var rule *IdxRuleConfig
	if idx, ok := this.cfgMap[table]; ok {
		rule = &idx
	}
	return rule
}

func (this *Aggregator) SetRule(idxRulesCfg []IdxRuleConfig) {
	if this.cfgMap == nil {
		this.cfgMap = make(map[string]IdxRuleConfig)
	}
	for _, val := range idxRulesCfg {
		for _, v := range val.Tables {
			this.cfgMap[v] = val
		}
	}
}

func (this *Aggregator) DiffData(idxR *IdxRuleConfig, dataA map[string]interface{}, dataB map[string]interface{}) (ret map[string]interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			switch x := e.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New(IDXRULE_ERR2)
			}
		}
	}()
	ret = make(map[string]interface{})
	for key, valA := range dataA {
		if valB, ok := dataB[key]; ok {
			if valA != valB {
				ret[key] = valA
			}
		} else {
			ret[key] = valA
		}
	}
	if idxR.ExcludeField != nil {
		for _, key := range idxR.ExcludeField {
			if _, ok := ret[key]; ok {
				delete(ret, key)
			}
		}
	}
	if idxR.PrimaryKey != "" {
		if primaryValue, ok := dataA[idxR.PrimaryKey]; ok && len(ret) > 0 {
			ret[idxR.PrimaryKey] = primaryValue
		}
	}
	if len(ret) == 0 {
		ret = nil
	}
	return
}
