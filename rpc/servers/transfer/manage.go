package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/rpc/pb"
	"github.com/always-waiting/cobra-canal/rules/transfer"
)

const (
	ERR_MSG1 = "没有发现%d号过滤manager"
)

type ManagerRPC struct {
	Obj map[int64]*transfer.Manager
}

func (this *ManagerRPC) GetManager(id int64) (*transfer.Manager, error) {
	m, ok := this.Obj[id]
	if !ok {
		return nil, errors.Errorf(ERR_MSG1, id)
	}
	return m, nil
}

func (this *ManagerRPC) GetCfg(ctx context.Context, in *pb.Request) (*pb.RespConfig, error) {
	status := &pb.Status{Code: 200, Message: "success"}
	id := in.Id
	manager, ok := this.Obj[id]
	if !ok {
		status.Code = 400
		status.Message = fmt.Sprintf(ERR_MSG1, id)
		return &pb.RespConfig{
			Status: status,
		}, errors.Errorf(ERR_MSG1, id)
	}
	cfgBytes, _ := json.Marshal(manager.Cfg)
	ret := &pb.RespConfig{
		Status: status,
		Config: cfgBytes,
	}
	return ret, nil
}
