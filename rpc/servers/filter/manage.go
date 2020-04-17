package filter

import (
	"context"
	"encoding/json"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rpc/pb"
	"github.com/always-waiting/cobra-canal/rules/filter"
)

const (
	ERR_MSG1 = "没有发现%d号过滤manager"
)

type ManagerRPC struct {
	Obj map[int64]*filter.Manager
}

func (this *ManagerRPC) GetManager(id int64) (*filter.Manager, error) {
	m, ok := this.Obj[id]
	if !ok {
		return nil, errors.Errorf(ERR_MSG1, id)
	}
	return m, nil
}

func (this *ManagerRPC) GetCfg(ctx context.Context, in *pb.Request) (*pb.RespConfig, error) {
	status := &pb.Status{Code: 200, Message: "success"}
	m, err := this.GetManager(in.Id)
	if err != nil {
		status.Code = 400
		status.Message = err.Error()
		return &pb.RespConfig{
			Status: status,
		}, err

	}
	cfgBytes, _ := json.Marshal(m.Cfg)
	ret := &pb.RespConfig{
		Status: status,
		Config: cfgBytes,
	}
	return ret, nil
}
