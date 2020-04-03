package config

import (
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-mysql/canal"
)

type configureV2 struct {
	CobraCfg *CobraConfig   `toml:"cobra" description:"监控从库的配置"`
	RulesCfg []RuleConfigV2 `toml:"rules" description:"监控规则工厂的配置"`
	path     string
}

type CobraConfig struct {
	*canal.Config
	DbCfg  *MysqlConfig            `toml:"db" description:"监控信息记录库"`
	LogCfg LogConfig               `toml:"log" description:"日志配置"`
	ErrCfg errors.ErrHandlerConfig `toml:"err" description:"错误处理配置"`
	Rebase bool                    `toml:"rebase"`
	Port   int                     `toml:"port"`
	Host   string                  `toml:"host"`
}
