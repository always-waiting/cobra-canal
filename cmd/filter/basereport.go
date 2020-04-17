package filter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/always-waiting/cobra-canal/rules/filter"
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
	fId, _ := cmd.Flags().GetInt64("id")
	host, _ := cmd.Flags().GetString("host")
	port, _ := cmd.Flags().GetString("port")
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := filter.NewFilterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.BaseReport(ctx, &filter.FilterRequest{Id: fId})
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
