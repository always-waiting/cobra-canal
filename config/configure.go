package config

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-mysql/canal"
	"github.com/spf13/viper"
	"path/filepath"
)

const (
	ERROR1            = "Configuration does not exist"
	ERROR2            = "未知错误类型(%#v)"
	IDXRULE_ERR1      = "从event.Event中没有获取到aggre_field指定的域(%s)"
	IDXRULE_ERR2      = "idxrule出现未知错误"
	IDXRULE_SEPARATOR = ","
	DEFAULT_BUFFER    = 1000
)

type ConfigureV2 struct {
	CobraCfg *CobraConfig   `toml:"cobra" description:"监控从库的配置" json:",omitempty"`
	RulesCfg []RuleConfigV2 `toml:"rules" description:"监控规则工厂的配置" json:",omitempty"`
	path     string
}

func (this *ConfigureV2) String() string {
	return this.path
}

var configV2 *ConfigureV2

func LoadV2(configFile string) {
	if configFile == "" {
		configFile = "/tmp/cobra.toml"
	}
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	configV2 = &ConfigureV2{}
	if err := viper.Unmarshal(configV2, func(m *mapstructure.DecoderConfig) {
		m.TagName = "toml"
	}); err != nil {
		panic(err)
	}
	configV2.path = viper.ConfigFileUsed()
}

func ConfigV2() *ConfigureV2 {
	if configV2 != nil {
		return configV2
	} else {
		panic(errors.New(ERROR1))
	}
}

func LoadTestCfg(absolutePath string) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.Errorf("%v", e)
		}
	}()
	path, err := filepath.Abs("./")
	if err != nil {
		return err
	}
	file := fmt.Sprintf("%s/%s", path, absolutePath)
	LoadV2(file)
	return
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
