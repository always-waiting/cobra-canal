package rules

import (
	"net/http"
)

func (this *Rule) ServeHTTPStop(rsp http.ResponseWriter, req *http.Request) {
	this.Log.Infof("收到信号，关闭规则%s", this.GetName())
	this.Close()
}

func (this *Rule) ServeHTTPStart(rsp http.ResponseWriter, req *http.Request) {
	this.Log.Infof("收到信号，开启规则%s", this.GetName())
	if !this.closed { //没有关闭，不用启动
		return
	}
	if err := this.Reset(); err != nil {
		return
	}
	go this.Start()
}
