package cobra

import (
	"fmt"
	"github.com/google/gops/agent"
	"net/http"
)

type CobraHttp struct {
	*http.Server
	Mux *http.ServeMux
}

func CreateCobraHttp(port int) (*CobraHttp, error) {
	r := &CobraHttp{}
	mux := http.NewServeMux()
	s := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", "127.0.0.1", port),
		Handler: mux,
	}
	r.Server = s
	r.Mux = mux
	return r, nil
}

func (this *CobraHttp) AddRulePath(h *Handler) (err error) {
	for _, r := range h.Rules {
		// 注册规则关闭路由
		this.Mux.HandleFunc(fmt.Sprintf("/rules/%s/stop", r.GetName()), r.ServeHTTPStop)
		// 注册规则开启路由
		this.Mux.HandleFunc(fmt.Sprintf("/rules/%s/start", r.GetName()), r.ServeHTTPStart)
	}
	this.Mux.HandleFunc("/rules/report", h.ServeHTTPReport)
	this.Mux.HandleFunc("/gops/debug/start", debugStart)
	this.Mux.HandleFunc("/gops/debug/stop", debugStop)
	return nil
}

func debugStart(rsp http.ResponseWriter, req *http.Request) {
	agent.Listen(agent.Options{})
}

func debugStop(rsp http.ResponseWriter, req *http.Request) {
	agent.Close()
}

func (this *CobraHttp) Run() error {
	return this.Server.ListenAndServe()
}

func (this *CobraHttp) Close() error {
	return this.Server.Close()
}
