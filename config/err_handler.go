package config

import (
	"encoding/json"
	"github.com/always-waiting/cobra-canal/errors"
)

type errSenderConfig map[string]string

func (this errSenderConfig) Parse() errors.Sender {
	var ret errors.Sender
	strType, ok := this["type"]
	if !ok {
		strType = "fake"
	}
	switch strType {
	case "fake":
		ret = errors.FakeSender{}
	case "sentinel":
		jsonStr, _ := json.Marshal(this)
		sen := SentinelConfig{}
		json.Unmarshal(jsonStr, &sen)
		ret = &sen
	default:
		panic("未定义的错误处理类型" + strType)
	}
	return ret
}
