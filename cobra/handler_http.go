package cobra

import (
	"net/http"

	"github.com/MakeNowJust/heredoc"
	"github.com/always-waiting/cobra-canal/rules"
)

const (
	REPORT_TEMPLATE = "%-30s\t%-15s\t%-15s\t%-15s\t%-15s"
)

var REPORT_TEMPLATE1 = heredoc.Doc(`
>>>>>>>>>>规则%s<<<<<<<<<<
$$$$$基本信息$$$$$
是否运行: %v
是否聚合: %v
$$$$$过滤池信息$$$$$
过滤池容量: %d
过滤池事件数: %d
$$$$$聚合器信息$$$$$
聚合时间: %s
聚合键个数: %d
$$$$$过滤器信息$$$$$
%s`)

var REPORT_RULER = heredoc.Doc(`
#####Number: %d#####
是否运行: %v
%s`)

var REPORT_CONSUMER = heredoc.Doc(`
$$$$$消费器: %s$$$$$
消费池容量: %d
消费池事件数: %d
消费器个数: %s`)

func (h *Handler) ServeHTTPReport(rsp http.ResponseWriter, req *http.Request) {
	rsp.Header().Set("Content-Type", "application/json")
	ret := stdReturn{}
	err := req.ParseForm()
	var infos []rules.FactoryInfo
	var name string
	if err != nil {
		ret.Code = 500
		ret.Message = FAIL
		ret.Err = err.Error()
		goto RETURN
	}
	name = req.Form.Get("rule")
	infos, err = h.reportRule(name)
	if err != nil {
		ret.Code = 500
		ret.Message = FAIL
		ret.Err = err.Error()
		goto RETURN
	}
	ret.Code = 200
	ret.Message = SUCCESS
	ret.Data = infos
RETURN:
	js, _ := ret.json(req)
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
