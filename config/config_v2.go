package config

import (
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-mysql/canal"
)

type ConfigureV2 struct {
	CobraCfg *CobraConfig   `toml:"cobra" description:"监控从库的配置" json:",omitempty"`
	RulesCfg []RuleConfigV2 `toml:"rules" description:"监控规则工厂的配置" json:",omitempty"`
	path     string
}

func (this *ConfigureV2) String() string {
	return this.path
}

type CobraConfig struct {
	*canal.Config
	DbCfg  *MysqlConfig             `toml:"db" description:"监控信息记录库" json:",omitempty"`
	LogCfg *LogConfig               `toml:"log" description:"日志配置" json:",omitempty"`
	ErrCfg *errors.ErrHandlerConfig `toml:"err" description:"错误处理配置" json:",omitempty"`
	Rebase bool                     `toml:"rebase"`
	Host   string                   `toml:"host"`
	Buffer int                      `toml:"buffer"`
}

func (this *CobraConfig) GetLogCfg() *LogConfig {
	if this.LogCfg != nil {
		return this.LogCfg
	}
	return DefaultLogCfg
}

func (this *CobraConfig) GetBuffer() int {
	if this.Buffer == 0 {
		this.Buffer = 500
	}
	return this.Buffer
}
