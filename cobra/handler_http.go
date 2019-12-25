package cobra

import (
	"fmt"
	"net/http"
	"strings"
)

const (
	REPORT_TEMPLATE = "%-30s\t%-15s\t%-15s\t%-15s\t%-15s"
)

func (h *Handler) ServeHTTPReport(rsp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		fmt.Println("解析出错")
		return
	}
	name := req.Form.Get("rule")
	ruleinfo, err := h.reportRule(name)
	if err != nil {
		rsp.Write([]byte(err.Error()))
	} else {
		rsp.Write(ruleinfo)
	}
}

func (h *Handler) reportRule(name string) (ret []byte, err error) {
	var info string
	lineSep := "\n"
	head := []interface{}{"name", "aggreable", "closed", "ruler", "consumer"}
	report := []string{fmt.Sprintf(REPORT_TEMPLATE, head...)}
	for _, r := range h.Rules {
		if name == r.GetName() || name == "all" {
			cols := make([]interface{}, 0)
			cols = append(cols, r.GetName())
			cols = append(cols, fmt.Sprintf("%v", r.IsAggre()))
			cols = append(cols, fmt.Sprintf("%v", r.IsClosed()))
			cols = append(cols, fmt.Sprintf("%d/%d", r.ActiveRulerNum(), r.RulerNum()))
			csrInfo, err := r.ReportConsumer()
			if err != nil {
				return nil, err
			}
			cols = append(cols, strings.Join(csrInfo, ","))
			report = append(report, fmt.Sprintf(REPORT_TEMPLATE, cols...))
			if name != "all" {
				break
			}
		}
	}
	info = strings.Join(report, lineSep)
	ret = []byte(info)
	return
}
