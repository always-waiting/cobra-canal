package positions

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:     "report",
	Short:   "报告同步程序处理到的mysql binlog位置",
	Version: "1.0.0",
	Run:     reportCmdRun,
}

func reportCmdRun(cmd *cobra.Command, args []string) {
	port, err := helps.GetPort(cmd)
	if err != nil {
		panic(err)
	}
	host, _ := cmd.Flags().GetString("host")
	pretty, _ := cmd.Flags().GetBool("pretty")
	var Addr string
	if pretty {
		Addr = fmt.Sprintf("http://%s:%s/cobra/position?pretty", host, port)
	} else {
		Addr = fmt.Sprintf("http://%s:%s/cobra/position", host, port)
	}
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
	fmt.Println(string(body))
}
