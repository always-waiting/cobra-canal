package config

import (
	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/errors"
)

type RuleConfigV2 struct {
	Id             int                     `toml:"id"`
	Desc           string                  `toml:"desc"`
	QueueAddr      string                  `toml:"queue_addr"`
	LogCfg         LogConfig               `toml:"log"`
	DbCfg          *MysqlConfig            `toml:"db"`
	ErrCfg         errors.ErrHandlerConfig `toml:"err"`
	FilterManage   FilterManageConfig      `toml:"filtermanage"`
	TransferManage TransferManageConfig    `toml:"transfermanage"`
	ConsumeManage  ConsumeManageConfig     `toml:"consumemanage"`
	Port           int                     `toml:"port"`
}

type FilterManageConfig struct {
	Name           string                  `toml:"name"`
	Desc           string                  `toml:"desc"`
	Percent        int                     `toml:"percent"`
	DbRequired     bool                    `toml:"db_required"`
	Worker         WorkerConfig            `toml:"worker"`
	TableFilterCfg *TableFilterConfig      `toml:"tablefilter"`
	AggreCfg       *collection.AggreConfig `toml:"aggregation"`
}

func (this *FilterManageConfig) HasTableFilter() bool {
	return this.TableFilterCfg != nil
}

func (this *FilterManageConfig) IsAggreable() bool {
	return this.AggreCfg != nil
}

type TransferManageConfig struct {
	Name       string       `toml:"name"`
	Desc       string       `toml:"desc"`
	Percent    int          `toml:"percent"`
	DbRequired bool         `toml:"db_required"`
	Worker     WorkerConfig `toml:"worker"`
}

type ConsumeManageConfig struct {
	Name       string         `toml:"name"`
	Desc       string         `toml:"desc"`
	Percent    int            `toml:"percent"`
	DbRequired bool           `toml:"db_required"`
	Workers    []WorkerConfig `toml:"workers"`
}

type WorkerConfig map[string]interface{}
