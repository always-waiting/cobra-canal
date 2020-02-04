package config

import (
	"fmt"
	"github.com/juju/errors"
)

const (
	MYSQLCONFIG_ERR1   = "参数不能为空"
	MYSQLCONFIG_GORM   = "%s:%s@(%s)/%s?charset=utf8&parseTime=True&loc=Local"
	MYSQLCONFIG_REPORT = "%s/%s"
)

type MysqlConfig struct {
	Addr   string `toml:"addr"`
	User   string `toml:"user"`
	Passwd string `toml:"password" json:"-"`
	Db     string `toml:"db"`
}

func (m *MysqlConfig) ToGormAddr() (string, error) {
	err := m.CheckParams()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(MYSQLCONFIG_GORM, m.User, m.Passwd, m.Addr, m.Db), nil
}

func (m *MysqlConfig) CheckParams() error {
	if m.Addr == "" ||
		m.User == "" ||
		m.Passwd == "" ||
		m.Db == "" {
		return errors.New(MYSQLCONFIG_ERR1)
	}
	return nil
}

func (m *MysqlConfig) Report() string {
	return fmt.Sprintf(MYSQLCONFIG_REPORT, m.Addr, m.Db)
}
