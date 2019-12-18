package config

import (
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

const (
	ERROR1            = "Configuration does not exist"
	ERROR2            = "未知错误类型(%#v)"
	IDXRULE_ERR1      = "从event.Event中没有获取到aggre_field指定的域(%s)"
	IDXRULE_ERR2      = "idxrule出现未知错误"
	IDXRULE_SEPARATOR = ","
)

var config *configure

func Load(configFile string) {
	if configFile == "" {
		configFile = "/tmp/cobra.toml"
	}
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	config = &configure{}
	if err := viper.Unmarshal(config, func(m *mapstructure.DecoderConfig) {
		m.TagName = "toml"
	}); err != nil {
		panic(err)
	}
	config.path = viper.ConfigFileUsed()
}

func Config() *configure {
	if config != nil {
		return config
	} else {
		panic(errors.New(ERROR1))
	}
}

func NewConfig() configure {
	return configure{}
}
