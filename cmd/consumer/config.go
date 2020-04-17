package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/always-waiting/cobra-canal/rpc/pb"
	"github.com/always-waiting/cobra-canal/rpc/pb/consumer"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"time"
)

var getCfgCmd = &cobra.Command{
	Use:     "config",
	Short:   "监控组建基本信息",
	Version: "2.0.0",
	Run:     getCfgCmdRun,
}

func getCfgCmdRun(cmd *cobra.Command, args []string) {
	fId, _ := cmd.Flags().GetInt64("id")
	host, _ := cmd.Flags().GetString("host")
	port, _ := cmd.Flags().GetString("port")
	address := fmt.Sprintf("%s:%s", host, port)
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	c := consumer.NewManageClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.GetCfg(ctx, &pb.Request{Id: fId})
	if err != nil {
		panic(err)
	}
	cfg := make(map[string]interface{})
	json.Unmarshal(r.Config, &cfg)
	report := struct {
		Status *pb.Status             `json:"status"`
		Config map[string]interface{} `json:"config"`
	}{Status: r.Status, Config: cfg}
	pretty, _ := cmd.Flags().GetBool("pretty")
	var pjson []byte
	if pretty {
		pjson, _ = json.MarshalIndent(report, "", "\t")
	} else {
		pjson, _ = json.Marshal(report)
	}
	fmt.Println(string(pjson))
}
