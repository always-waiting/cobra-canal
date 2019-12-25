package rule

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:     "report",
	Short:   "提供规则信息",
	Version: "1.0.0",
	Run:     reportCmdRun,
}

func init() {
	reportCmd.Flags().String("rule", "all", "规则名称")
}

func reportCmdRun(cmd *cobra.Command, args []string) {
	port, err := getPort(cmd)
	if err != nil {
		panic(err)
	}
	rulename, _ := cmd.Flags().GetString("rule")
	Addr := fmt.Sprintf("http://127.0.0.1:%s/rules/report?rule=%s", port, rulename)
	req, _ := http.NewRequest("GET", Addr, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if rulename != "all" {
		fmt.Println("可以开发新的专门获取规则详情的命令，目前先以简介为基础")
	}
	fmt.Println(string(body))
}
