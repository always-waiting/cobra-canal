package positions

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var saveCmd = &cobra.Command{
	Use:     "save",
	Short:   "把同步位置存储到数据库",
	Version: "1.0.0",
	Run:     saveCmdRun,
}

func saveCmdRun(cmd *cobra.Command, args []string) {
	port, err := helps.GetPort(cmd)
	if err != nil {
		panic(err)
	}
	host, _ := cmd.Flags().GetString("host")
	pretty, _ := cmd.Flags().GetBool("pretty")
	var Addr string
	if pretty {
		Addr = fmt.Sprintf("http://%s:%s/cobra/position/save?pretty", host, port)
	} else {
		Addr = fmt.Sprintf("http://%s:%s/cobra/position/save", host, port)
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
