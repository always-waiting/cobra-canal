package collection

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
)

const (
	IDXRULE_SEPARATOR = ","
	IDXRULE_ERR1      = "从event.Event中没有获取到aggre_field指定的域(%s)"
)

type AggreConfig struct {
	Time        int             `toml:"time" description:"缓存秒数" mapstructure:"time"`
	IdxRulesCfg []IdxRuleConfig `toml:"idxrule" description:"缓存键的生成规则" mapstructure:"idxrule"`
}

type IdxRuleConfig struct {
	Tables       []string `toml:"tables" description:"记录日志的表" mapstructure:"tables"`
	IdxField     string   `toml:"idx_field" description:"日志唯一key字段" mapstructure:"idx_field"`
	IdxPrefix    string   `toml:"idx_prefix" description:"日志唯一key前缀" mapstructure:"idx_prefix"`
	IdxType      string   `toml:"idx_type" description:"日志唯一key字段类型" mapstructure:"idx_type"`
	AggreField   string   `toml:"aggre_field" description:"用于多表关联的聚合域" mapstructure:"aggre_field"`
	UserField    string   `toml:"user_field" description:"操作人字段" mapstructure:"user_field"`
	PrimaryKey   string   `toml:"primary_key" description:"表主键字段" mapstructure:"primary_key"`
	ExcludeField []string `toml:"exclude_field" description:"表主键字段" mapstructure:"exclude_field"`
}

func CreateByMap(input map[string]interface{}) (*Aggregator, error) {
	cfg := AggreConfig{}
	err := mapstructure.Decode(input, &cfg)
	if err != nil {
		return nil, err
	}
	return CreateAggregator(&cfg)
}

func (this IdxRuleConfig) Idx(e event.EventV2) (ret string, err error) {
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
