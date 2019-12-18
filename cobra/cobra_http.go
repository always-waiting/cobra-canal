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
		this.Mux.Handle(fmt.Sprintf("/rule/%s", r.GetName()), r)
	}
	return nil
}

func (this *CobraHttp) Run() error {
	return this.Server.ListenAndServe()
}

func (this *CobraHttp) Close() error {
	return this.Server.Close()
}
