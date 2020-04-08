package config

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
	"path/filepath"
)

const (
	ERROR1            = "Configuration does not exist"
	ERROR2            = "未知错误类型(%#v)"
	IDXRULE_ERR1      = "从event.Event中没有获取到aggre_field指定的域(%s)"
	IDXRULE_ERR2      = "idxrule出现未知错误"
	IDXRULE_SEPARATOR = ","
)

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
