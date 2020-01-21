package rules

import (
	"net/http"
)

func (this *Factory) ServeHTTPStop(rsp http.ResponseWriter, req *http.Request) {
	this.Log.Infof("收到信号，关闭规则%s", this.GetName())
	this.Close()
}

func (this *Factory) ServeHTTPStart(rsp http.ResponseWriter, req *http.Request) {
	this.Log.Infof("收到信号，开启规则%s", this.GetName())
	if !this.closed { //没有关闭，不用启动
		return
	}
	if err := this.Reset(); err != nil {
		return
	}
	this.Log.Info("重置成功")
	go this.Start()
}

type FactoryInfo struct {
	Name      string      `json:"name"`
	Aggreable bool        `json:"aggreable"`
	Closed    bool        `json:"closed"`
	EventNum  int         `json:"event_number"`
	EventCap  int         `json:"event_capacity"`
	Rulers    []RulerInfo `json:"rulers"`
	// 聚合信息暂时不考虑
}

func (this *Factory) Info() (info FactoryInfo, err error) {
	info = FactoryInfo{}
	info.Name = this.name
	info.Aggreable = this.IsAggre()
	info.Closed = this.IsClosed()
	info.EventNum = len(this.eventChannel)
	info.EventCap = cap(this.eventChannel)
	rInfos := make([]RulerInfo, 0)
	for _, r := range this.ruler {
		if rInfo, err := r.RulerInfo(); err != nil {
			return info, err
		} else {
			rInfos = append(rInfos, rInfo)
		}
	}
	info.Rulers = rInfos
	return
}
