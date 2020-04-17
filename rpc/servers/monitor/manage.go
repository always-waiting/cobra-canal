package monitor

import (
	"context"
	"encoding/json"
	"github.com/always-waiting/cobra-canal/monitor"
	"github.com/always-waiting/cobra-canal/rpc/pb"
)

type MonitorRPC struct {
	Obj *monitor.Monitor
}

func (this MonitorRPC) GetCfg(ctx context.Context, in *pb.Request) (*pb.RespConfig, error) {
	cfg := this.Obj.Cfg()
	status := &pb.Status{Code: 200, Message: "success"}
	cfgBytes, _ := json.Marshal(cfg)
	ret := &pb.RespConfig{
		Status: status,
		Config: cfgBytes,
	}
	return ret, nil
}
