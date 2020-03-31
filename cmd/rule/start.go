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
	pretty, _ := cmd.Flags().GetBool("pretty")
	var Addr string
	if pretty {
		Addr = fmt.Sprintf("http://%s:%s/rules/%s/start?pretty", host, port, rulename)
	} else {
		Addr = fmt.Sprintf("http://%s:%s/rules/%s/start", host, port, rulename)
	}
	req, _ := http.NewRequest("GET", Addr, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	helps.CmdPrint(resp)
}
