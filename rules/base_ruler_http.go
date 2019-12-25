package rules

import (
	"fmt"
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
