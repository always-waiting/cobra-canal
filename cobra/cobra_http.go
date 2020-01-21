package cobra

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/always-waiting/cobra-canal/config"
	"github.com/google/gops/agent"
)

type CobraHttp struct {
	*http.Server
	Mux   *http.ServeMux
	cobra *Cobra
}

const (
	SUCCESS = "success"
	FAIL    = "fail"
)

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
	// 注册规则运行报告
	this.Mux.HandleFunc("/rules/report", h.ServeHTTPReport)
	// 开启调试模式
	this.Mux.HandleFunc("/gops/debug/start", debugStart)
	// 关闭调试模式
	this.Mux.HandleFunc("/gops/debug/stop", debugStop)
	// 监控位置报告
	this.Mux.HandleFunc("/cobra/position", this.reportPosition)
	// 监控位置存储
	this.Mux.HandleFunc("/cobra/position/save", this.savePosition)
	return nil
}

type stdReturn struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Err     interface{} `json:"err,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

func (this stdReturn) json(req *http.Request) ([]byte, error) {
	q := req.URL.Query()
	_, flag := q["pretty"]
	if flag {
		return json.MarshalIndent(this, "", "\t")
	}
	return json.Marshal(this)
}

func (this *CobraHttp) reportPosition(rsp http.ResponseWriter, req *http.Request) {
	cfg := config.Config()
	pos := this.cobra.syncedPosition()
	rsp.Header().Set("Content-Type", "application/json")
	data := stdReturn{Code: 200, Message: SUCCESS, Data: struct {
		ServerID uint32 `json:"server_id"`
		Name     string `json:"name"`
		Pos      uint32 `json:"pos"`
	}{cfg.CanalCfg.ServerID, pos.Name, pos.Pos}}
	js, _ := data.json(req)
	rsp.Write(js)
}

func (this *CobraHttp) savePosition(rsp http.ResponseWriter, req *http.Request) {
	err := this.cobra.SavePosition()
	var data stdReturn
	if err != nil {
		data = stdReturn{Code: 500, Message: FAIL, Err: err}
	} else {
		data = stdReturn{Code: 200, Message: SUCCESS}
	}
	js, _ := data.json(req)
	rsp.Write(js)
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
