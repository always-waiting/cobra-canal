package config

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"reflect"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

func makeDefaultAggregatorV2(r *RuleConfig) *AggregatorV2 {
	ret := &AggregatorV2{}
	ret.CIdxGenerator = ret.defaultCIdxGenerator
	ret.SetRule(r.AggreCfg.IdxRulesCfg)
	ret.Collector = makeCollectorV2(r.AggreCfg)
	return ret
}

type AggregatorV2 struct {
	cfgMap        map[string]IdxRuleConfig
	CIdxGenerator func(event.Event) (string, error)
	Collector     *CollectorV2
}

func (b *AggregatorV2) GetAggreInfo() AggreInfo {
	return AggreInfo{Interval: b.GetTimeDuration(), Number: b.GetKeyNum()}
}

func (b *AggregatorV2) GetKeyNum() int {
	return len(b.Collector.Data)
}

func (b *AggregatorV2) GetTimeDuration() string {
	return fmt.Sprintf("%v", b.Collector.slotNum)
}

func (b *AggregatorV2) Stop() {
	b.Collector.Stop()
}

func (b *AggregatorV2) Reset() {
}

func (this *AggregatorV2) AppendEvent(key string, e event.Event) error {
	return this.Collector.AddEvent(key, e)
}

func (this *AggregatorV2) CreateEvent(key string, e event.Event) error {
	return this.Collector.AddEvent(key, e)
}

func (this *AggregatorV2) MoveEvents(key string) ([]event.Event, error) {
	return nil, nil
}

func (this *AggregatorV2) HasIdx(key string) bool {
	return false
}

func (this *AggregatorV2) GetSendChanV2() chan []event.Event {
	return this.Collector.SendChan
}

func (this *AggregatorV2) GetSendChan() chan string {
	return nil
}

func (this *AggregatorV2) GetIdxValue(e event.Event) (string, error) {
	return this.CIdxGenerator(e)
}

func (this *AggregatorV2) defaultCIdxGenerator(e event.Event) (retStr string, err error) {
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

func (this *AggregatorV2) defaultIdxRuleParser(idxR *IdxRuleConfig, e event.Event) (ret string, err error) {
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

func (this *AggregatorV2) GetRule(table string) *IdxRuleConfig {
	var rule *IdxRuleConfig
	if idx, ok := this.cfgMap[table]; ok {
		rule = &idx
	}
	return rule
}

func (this *AggregatorV2) SetRule(idxRulesCfg []IdxRuleConfig) {
	if this.cfgMap == nil {
		this.cfgMap = make(map[string]IdxRuleConfig)
	}
	for _, val := range idxRulesCfg {
		for _, v := range val.Tables {
			this.cfgMap[v] = val
		}
	}
}

func (this *AggregatorV2) DiffData(idxR *IdxRuleConfig, dataA map[string]interface{}, dataB map[string]interface{}) (ret map[string]interface{}, err error) {
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
			if !reflect.DeepEqual(valA, valB) {
				ret[key] = valA
			}
			/*
				if valA != valB {
					ret[key] = valA
				}
			*/
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
