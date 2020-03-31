package cobra

import (
	"net/http"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/always-waiting/cobra-canal/rules"
)

func (h *Handler) ServeHTTPReport(rsp http.ResponseWriter, req *http.Request) {
	rsp.Header().Set("Content-Type", "application/json")
	ret := helps.StdReturn{}
	err := req.ParseForm()
	var infos []rules.FactoryInfo
	var name string
	if err != nil {
		ret.Code = 500
		ret.Message = helps.HTTPFAIL
		ret.Err = err.Error()
		goto RETURN
	}
	name = req.Form.Get("rule")
	infos, err = h.reportRule(name)
	if err != nil {
		ret.Code = 500
		ret.Message = helps.HTTPFAIL
		ret.Err = err.Error()
		goto RETURN
	}
	ret.Code = 200
	ret.Message = helps.HTTPSUCCESS
	ret.Data = infos
RETURN:
	js, _ := ret.Json(req)
	rsp.Write(js)
}

func (h *Handler) reportRule(name string) (ret []rules.FactoryInfo, err error) {
	ret = make([]rules.FactoryInfo, 0)
	for _, r := range h.Rules {
		if name == "all" || name == r.GetName() {
			if info, err := r.Info(); err != nil {
				return nil, err
			} else {
				ret = append(ret, info)
			}
		}
	}
	return
}
