package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/always-waiting/cobra-canal/monitor"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

var baseReportCmd = &cobra.Command{
	Use:     "basereport",
	Short:   "监控组建基本信息",
	Version: "2.0.0",
	Run:     baseReportCmdRun,
}

func baseReportCmdRun(cmd *cobra.Command, args []string) {
	host, _ := cmd.Flags().GetString("host")
	port, _ := cmd.Flags().GetString("port")
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := monitor.NewMonitorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.BaseReport(ctx, &monitor.MonitorRequest{})
	if err != nil {
		panic(err)
	}
	pretty, _ := cmd.Flags().GetBool("pretty")
	var pjson []byte
	if pretty {
		pjson, _ = json.MarshalIndent(r, "", "\t")
	} else {
		pjson, _ = json.Marshal(r)
	}
	fmt.Println(string(pjson))
}
