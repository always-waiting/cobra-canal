package monitor

import (
	"context"
)

type MonitorRPC struct {
	Obj *Monitor
}

func (this *MonitorRPC) BaseReport(ctx context.Context, in *MonitorRequest) (*BaseInfo, error) {
	serverId := uint64(this.Obj.cfg.CobraCfg.ServerID)
	port := uint32(this.Obj.cfg.CobraCfg.GetPort())
	slave := this.Obj.cfg.CobraCfg.Addr
	include := this.Obj.cfg.CobraCfg.IncludeTableRegex
	exclude := this.Obj.cfg.CobraCfg.ExcludeTableRegex
	return &BaseInfo{
		Status: &Status{Code: 200, Message: "success"},
		Data: &BaseInfo_Data{
			Slave: slave, Port: port, ServerId: serverId,
			IncludeTableRegex: include, ExcludeTableRegex: exclude,
		},
	}, nil
}
