package errors

import "encoding/json"

var DefaultErr = ErrHandlerConfig(map[string]string{"type": "fake"})

type ErrHandlerConfig map[string]string

func (this ErrHandlerConfig) MakeHandler() ErrHandlerV2 {
	var ret ErrHandlerV2
	strType, ok := this["type"]
	if !ok {
		strType = "fake"
	}
	var sender Sender
	switch strType {
	case "fake":
		sender = FakeSender{}
	case "sentinel":
		jsonStr, _ := json.Marshal(this)
		sen := &SentinelConfig{}
		json.Unmarshal(jsonStr, sen)
		sender = sen
	default:
		panic("未定义的错误处理类型" + strType)
	}
	ret.Init()
	ret.SetSender(sender)
	return ret
}
