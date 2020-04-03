package collection

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
)

const (
	IDXRULE_SEPARATOR = ","
	IDXRULE_ERR1      = "从event.Event中没有获取到aggre_field指定的域(%s)"
)

type AggreConfig struct {
	Time        int             `toml:"time" description:"缓存秒数"`
	IdxRulesCfg []IdxRuleConfig `toml:"idxrule" description:"缓存键的生成规则"`
}

type IdxRuleConfig struct {
	Tables       []string `toml:"tables" description:"记录日志的表"`
	IdxField     string   `toml:"idx_field" description:"日志唯一key字段"`
	IdxPrefix    string   `toml:"idx_prefix" description:"日志唯一key前缀"`
	IdxType      string   `toml:"idx_type" description:"日志唯一key字段类型"`
	AggreField   string   `toml:"aggre_field" description:"用于多表关联的聚合域"`
	UserField    string   `toml:"user_field" description:"操作人字段"`
	PrimaryKey   string   `toml:"primary_key" description:"表主键字段"`
	ExcludeField []string `toml:"exclude_field" description:"表主键字段"`
}

func (this IdxRuleConfig) Idx(e event.Event) (ret string, err error) {
	var idxField, actionField interface{}
	if idxField, err = e.GetColumnValue(0, this.IdxField); err != nil {
		return
	}
	ret = fmt.Sprintf("%v", idxField)
	if this.IdxPrefix != "" {
		switch this.IdxPrefix {
		case "TABLENAME":
			ret = fmt.Sprintf("%s:%s", e.Table.Name, ret)
		default:
			ret = fmt.Sprintf("%s:%s", this.IdxPrefix, ret)
		}
	}
	if this.AggreField != "" {
		if actionField, err = e.GetColumnValue(len(e.RawData)-1, this.AggreField); err != nil {
			return
		}
		if actionField != nil {
			ret = fmt.Sprintf("%v%s%s", actionField, IDXRULE_SEPARATOR, ret)
		} else {
			err = errors.Errorf(IDXRULE_ERR1, this.AggreField)
		}
	}
	return
}
