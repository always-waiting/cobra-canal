package rules

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/consumes"
	"net/http"
)

/*
这个层级是对某一个worker的操作
例如关闭规则A的第一个worker
*/

// 关闭规则
func (this *BasicRuler) ServeHTTPStop(rsp http.ResponseWriter, req *http.Response) {
	fmt.Println("为端口监听提供支持")
}

type RulerInfo struct {
	Name     string                 `json:"-"`
	Desc     string                 `json:"-"`
	Id       int                    `json:"id"`
	Closed   bool                   `json:"closed"`
	Consumes []consumes.FactoryInfo `json:"consumes"`
}

func (this *BasicRuler) RulerInfo() (info RulerInfo, err error) {
	info = RulerInfo{}
	info.Name = this.name
	info.Desc = this.desc
	info.Id = this.number
	info.Closed = this.IsClosed()
	csfInfos := make([]consumes.FactoryInfo, 0)
	for _, csf := range this.consumers {
		if csfInfo, err := csf.FactoryInfo(); err != nil {
			return info, err
		} else {
			csfInfos = append(csfInfos, csfInfo)
		}
	}
	info.Consumes = csfInfos
	return
}
