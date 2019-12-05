package config

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
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
	var data []byte
	var err error
	if data, err = ioutil.ReadFile(configFile); err != nil {
		panic(err)
	}
	if _, err = toml.Decode(string(data), &config); err != nil {
		panic(err)
	}
	/*
		if err = config.SetLog(); err != nil {
			panic(err)
		}
	*/
	if path, err := filepath.Abs(filepath.Dir(configFile)); err != nil {
		panic(err)
	} else {
		_, filename := filepath.Split(configFile)
		config.path = fmt.Sprintf("%s/%s", path, filename)
	}
}

func Config() *configure {
	if config != nil {
		return config
	} else {
		panic(errors.New(ERROR1))
	}
}
