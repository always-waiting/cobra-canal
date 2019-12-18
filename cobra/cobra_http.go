package cobra

import (
	"fmt"
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
	return nil
}

func (this *CobraHttp) Run() error {
	return this.Server.ListenAndServe()
}

func (this *CobraHttp) Close() error {
	return this.Server.Close()
}
