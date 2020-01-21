package cobra

import (
	"fmt"
	"github.com/MakeNowJust/heredoc"
	"github.com/always-waiting/cobra-canal/rules"
	"net/http"
	"strings"
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
	err := req.ParseForm()
	if err != nil {
		fmt.Println("解析出错")
		return
	}
	var ruleinfo []byte
	name := req.Form.Get("rule")
	if name == "all" {
		ruleinfo, err = h.reportRuleAll()
	} else {
		ruleinfo, err = h.reportRule(name)
	}
	if err != nil {
		rsp.Write([]byte(err.Error()))
	} else {
		rsp.Write(ruleinfo)
	}
}

func (h *Handler) reportRuleAll() (ret []byte, err error) {
	var info string
	lineSep := "\n"
	head := []interface{}{"name", "aggreable", "closed", "ruler", "consumer"}
	report := []string{fmt.Sprintf(REPORT_TEMPLATE, head...)}
	for _, r := range h.Rules {
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
	}
	info = strings.Join(report, lineSep)
	ret = []byte(info)
	return
}

func (h *Handler) reportRule(name string) (ret []byte, err error) {
	var r *rules.Factory
	for _, a := range h.Rules {
		if a.GetName() == name {
			r = a
			break
		}
	}
	if r == nil {
		ret = []byte(fmt.Sprintf("没有找到规则%s", name))
		return
	}
	isClose := r.IsClosed()
	isAggre := r.IsAggre()
	rulePoolCap := r.PoolCap()
	rulePoolLen := r.PoolLen()
	var aggreTime string
	var aggreNum int
	if isAggre {
		aggreNum = r.GetAggreKeyNum()
		aggreTime = r.GetAggreDuration()
	} else {
		aggreTime = "0s"
		aggreNum = 0
	}
	var ruleInfos []string
	for _, r := range r.GetRulers() {
		rNum := r.GetNumber()
		csrMap := r.CsrNum()
		activecsrMap := r.ActiveCsrNum()
		csrPoolCapMap := r.CsrPoolCap()
		csrPoolLenMap := r.CsrPoolLen()
		csrInfo := make([]string, 0)
		isClosed := r.IsClosed()
		for name, total := range csrMap {
			active := activecsrMap[name]
			capNum := csrPoolCapMap[name]
			lenNum := csrPoolLenMap[name]
			info := fmt.Sprintf(REPORT_CONSUMER,
				name, capNum, lenNum, fmt.Sprintf("%d/%d", active, total),
			)
			csrInfo = append(csrInfo, info)
		}
		ruleInfo := fmt.Sprintf(REPORT_RULER,
			rNum, !isClosed, strings.Join(csrInfo, "\n"),
		)
		ruleInfos = append(ruleInfos, ruleInfo)
	}
	info := fmt.Sprintf(REPORT_TEMPLATE1,
		name, !isClose, isAggre,
		rulePoolCap, rulePoolLen,
		aggreTime, aggreNum,
		strings.Join(ruleInfos, "\n"),
	)
	ret = []byte(info)
	return
}
