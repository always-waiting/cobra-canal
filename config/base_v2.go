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

var configV2 *configureV2

func LoadV2(configFile string) {
	if configFile == "" {
		configFile = "/tmp/cobra.toml"
	}
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
	configV2 = &configureV2{}
	if err := viper.Unmarshal(configV2, func(m *mapstructure.DecoderConfig) {
		m.TagName = "toml"
	}); err != nil {
		panic(err)
	}
	configV2.path = viper.ConfigFileUsed()
}

func ConfigV2() *configureV2 {
	if configV2 != nil {
		return configV2
	} else {
		panic(errors.New(ERROR1))
	}
}
