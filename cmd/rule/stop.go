package rule

import (
	"fmt"

	"net/http"

	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:     "stop",
	Short:   "关闭某一规则",
	Version: "1.0.0",
	Run:     stopCmdRun,
}

func init() {
	stopCmd.Flags().String("rule", "", "规则名称")
	stopCmd.MarkFlagRequired("rule")
}

func stopCmdRun(cmd *cobra.Command, args []string) {
	port, err := getPort(cmd)
	if err != nil {
		panic(err)
	}
	rulename, _ := cmd.Flags().GetString("rule")
	Addr := fmt.Sprintf("http://127.0.0.1:%s/rules/%s/stop", port, rulename)
	req, _ := http.NewRequest("GET", Addr, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.Status == "200 OK" {
		fmt.Println("关闭成功")
	} else {
		fmt.Println("关闭失败")
	}
}
