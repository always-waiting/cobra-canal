package rule

import (
	"fmt"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
	"net/http"
)

var startCmd = &cobra.Command{
	Use:     "start",
	Short:   "开启某一规则",
	Version: "1.0.0",
	Run:     startCmdRun,
}

func startCmdRun(cmd *cobra.Command, args []string) {
	port, _ := cmd.Flags().GetString("port")
	if port == "" {
		pid, _ := cmd.Flags().GetString("pid")
		if pid == "" {
			panic(ERR1)
		}
		var err error
		if port, err = helps.GetPortByPid(pid); err != nil {
			panic(err)
		}
		if port == "" {
			panic(ERR2)
		}
	}
	rulename, _ := cmd.Flags().GetString("rule")
	Addr := fmt.Sprintf("http://127.0.0.1:%s/rules/%s/start", port, rulename)
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
