package config

import (
	"github.com/mitchellh/mapstructure"
)

type TableFilterable interface {
	IsTablePass(string, string) bool
}

type TableFilterConfig struct {
	DbName  string   `toml:"db_name" mapstructure:"db_name"`
	Include []string `toml:"include_table" mapstructure:"include_table"`
	Exclude []string `toml:"exclude_table" mapstructure:"exclude_table"`
}

func CreateTableFilterByMap(input map[string]interface{}) (*TableFilterConfig, error) {
	cfg := TableFilterConfig{}
	err := mapstructure.Decode(input, &cfg)
	return &cfg, err
}

func (t *TableFilterConfig) IsTablePass(dbname string, table string) (flag bool) {
	if dbname != t.DbName {
		flag = false
		return
	}
	if t.Include != nil {
		for _, value := range t.Include {
			if table == value {
				flag = true
				break
			}
		}
	}

	if t.Exclude != nil {
		if t.Include == nil ||
			len(t.Include) == 0 {
			flag = true
		}
		for _, value := range t.Exclude {
			if table == value {
				flag = false
				break
			}
		}
	}
	if (t.Include == nil || len(t.Include) == 0) &&
		(t.Exclude == nil || len(t.Exclude) == 0) {
		flag = true
	}

	return
}
