package config

import (
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-log/log"
)

var (
	ErrManageCfgEmpty        = errors.New("manage属性没有定义")
	ErrAggreNotDefined       = errors.New("聚合器未定义")
	ErrTableFilterNotDefined = errors.New("表过滤器未定义")
)

var (
	ErrWorkerTypeNotFound = errors.New("没有发现对应的worker type")
	ErrOutOfIndex         = errors.New("下标越界")
)

type RuleConfigV2 struct {
	Id            int                      `toml:"id"`
	Desc          string                   `toml:"desc"`
	LineCfg       LineConfig               `toml:"line_config"`
	LogCfg        *LogConfig               `toml:"log" json:",omitempty"`
	DbCfg         *MysqlConfig             `toml:"db" json:",omitempty"`
	ErrCfg        *errors.ErrHandlerConfig `toml:"err" json:",omitempty"`
	FilterManage  *ManageConfig            `toml:"filtermanage" json:",omitempty"`
	ConsumeManage *ManageConfig            `toml:"consumemanage" json:",omitempty"`
	Compress      bool                     `toml:"compress"`
}

func (this RuleConfigV2) BuffNum(wt WorkerType) (int, error) {
	return wt.BuffNum(this)
}

func (this RuleConfigV2) ManagerName(workerType WorkerType) (string, error) {
	return workerType.ManagerName(this)
}

func (this RuleConfigV2) WorkerName(wt WorkerType, idx int) (string, error) {
	return wt.WorkerName(this, idx)
}

func (this RuleConfigV2) WorkersName(workerType WorkerType) ([]string, error) {
	return workerType.WorkersName(this)
}

func (this RuleConfigV2) Worker(wt WorkerType, idx int) (WorkerConfig, error) {
	return wt.Worker(this, idx)
}

func (this RuleConfigV2) Workers(wt WorkerType) ([]WorkerConfig, error) {
	return wt.Workers(this)
}

func (this RuleConfigV2) GetLogger(wt WorkerType) (*log.Logger, error) {
	return wt.GetLogger(this)
}

func (this RuleConfigV2) HasTableFilter() bool {
	return this.FilterManage.HasTableFilter()
}

func (this RuleConfigV2) TableFilter() *TableFilterConfig {
	return this.FilterManage.TableFilterCfg

}

func (this RuleConfigV2) ErrHandler() errors.ErrHandlerV2 {
	if this.ErrCfg != nil {
		return this.ErrCfg.MakeHandler()
	}
	return errors.DefaultErr.MakeHandler()
}
