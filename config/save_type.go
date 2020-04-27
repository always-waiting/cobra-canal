package config

import (
	"errors"
	"github.com/always-waiting/cobra-cancal/channels"
)

const (
	UndefinedSaveType = iota
	Memory
	Rabbitmq
)

var (
	ErrInterfaceAssertionFail = errors.New("interface断线失败")
	ErrSaveTypeNotFound       = errors.New("未定义的存储结构")
)

type SaveType int8

func (this SaveType) Session(cfg LineConfig) (ret Session, err error) {

}

type LineConfig map[string]interface{}

func (this LineConfig) SaveType() (ret SaveType, err error) {
	ret = UndefinedSaveType
	var typname string
	if typ, ok := this["type"]; !ok {
		return
	} else {
		var ok bool
		typname, ok = typ.(string)
		if !ok {
			err = ErrInterfaceAssertionFail
		}
	}
	switch typname {
	case "memory":
		ret = Memory
	case "rabbitmq":
		ret = Rabbitmq
	default:
		err = ErrSaveTypeNotFound
	}
	return
}
