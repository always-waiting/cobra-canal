package rule

import (
	"fmt"

	"net/http"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:     "start",
	Short:   "开启某一规则",
	Version: "1.0.0",
	Run:     startCmdRun,
}

func init() {
	startCmd.Flags().String("rule", "", "规则名称")
	startCmd.MarkFlagRequired("rule")
}

func startCmdRun(cmd *cobra.Command, args []string) {
	port, err := helps.GetPort(cmd)
	if err != nil {
		panic(err)
	}
	rulename, _ := cmd.Flags().GetString("rule")
	host, _ := cmd.Flags().GetString("host")
	Addr := fmt.Sprintf("http://%s:%s/rules/%s/start", host, port, rulename)
	req, _ := http.NewRequest("GET", Addr, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.Status == "200 OK" {
		fmt.Println("开启成功")
	} else {
		fmt.Println("开启失败")
	}
}
