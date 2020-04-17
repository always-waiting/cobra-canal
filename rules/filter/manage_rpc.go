package filter

import (
	"context"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/errors"
)

type ManagerRPC struct {
	Obj map[int64]*Manager
}

func (this *ManagerRPC) GetManager(id int64) (*Manager, error) {
	m, ok := this.Obj[id]
	if !ok {
		return nil, errors.Errorf("没法发现%d号过滤manager", id)
	}
	return m, nil
}

func (this *ManagerRPC) BaseReport(ctx context.Context, in *FilterRequest) (*BaseInfo, error) {
	manager, err := this.GetManager(in.Id)
	if err != nil {
		return &BaseInfo{
			Status: &Status{Code: 400, Message: "fail"},
		}, err
	}
	tableFilter := manager.Cfg.FilterManage.TableFilterCfg
	var tf *BaseInfo_TableFilter
	if tableFilter != nil {
		tf = &BaseInfo_TableFilter{}
		tf.Db = tableFilter.DbName
		tf.IncludeTable = tableFilter.Include
	}
	flag := manager.Cfg.IsAggreable()
	name, err := manager.Cfg.ManagerName(config.FilterWorker)
	if err != nil {
		return &BaseInfo{
			Status: &Status{Code: 400, Message: "fail"},
		}, err
	}
	return &BaseInfo{
		Status: &Status{Code: 200, Message: "success"},
		Data: &BaseInfo_Data{
			Filter:    tf,
			Aggreable: flag,
			Name:      name,
		},
	}, nil
}
