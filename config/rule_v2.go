package config

import (
	Err "errors"
	"fmt"
	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-log/log"
)

var (
	ErrManageCfgEmpty        = errors.New("manage属性没有定义")
	ErrAggreNotDefined       = errors.New("聚合器未定义")
	ErrTableFilterNotDefined = errors.New("表过滤器未定义")
)

type RuleConfigV2 struct {
	Id             int                      `toml:"id"`
	Desc           string                   `toml:"desc"`
	QueueAddr      string                   `toml:"queue_addr"`
	LogCfg         *LogConfig               `toml:"log" json:",omitempty"`
	DbCfg          *MysqlConfig             `toml:"db" json:",omitempty"`
	ErrCfg         *errors.ErrHandlerConfig `toml:"err" json:",omitempty"`
	FilterManage   *ManageConfig            `toml:"filtermanage" json:",omitempty"`
	TransferManage *ManageConfig            `toml:"transfermanage" json:",omitempty"`
	ConsumeManage  *ManageConfig            `toml:"consumemanage" json:",omitempty"`
	Compress       bool                     `toml:"compress"`
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

type ManageConfig struct {
	Name           string             `toml:"name"`
	Desc           string             `toml:"desc"`
	TableFilterCfg *TableFilterConfig `toml:"tablefilter" json:",omitempty"`
	Workers        []WorkerConfig     `toml:"workers" json:",omitempty"`
}

func (this *ManageConfig) HasTableFilter() bool {
	return this.TableFilterCfg != nil
}

type WorkerType int8

func (this WorkerType) TypeName(cfg WorkerConfig) (ret string) {
	var key string
	switch this {
	case FilterWorker:
		key = "filter_type"
	case TransferWorker:
		key = "transfer_type"
	case ConsumeWorker:
		key = "consume_type"
	}
	if val, ok := cfg[key]; ok {
		ret = val.(string)
	} else {
		ret = "base"
	}
	return
}

func (this WorkerType) ExchangeName() string {
	switch this {
	case FilterWorker:
		return "cobra_filter"
	case TransferWorker:
		return "cobra_transfer"
	case ConsumeWorker:
		return "cobra_consume"
	default:
		return ""
	}
}

func (this WorkerType) ManagerName(cfg RuleConfigV2) (ret string, err error) {
	switch this {
	case FilterWorker:
		ret = fmt.Sprintf("filter_%d_%s", cfg.Id, cfg.FilterManage.Name)
	case TransferWorker:
		ret = fmt.Sprintf("transfer_%d_%s", cfg.Id, cfg.TransferManage.Name)
	case ConsumeWorker:
		ret = fmt.Sprintf("consume_%d_%s", cfg.Id, cfg.ConsumeManage.Name)
	default:
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerType) WorkersName(cfg RuleConfigV2) (ret []string, err error) {
	ret = make([]string, 0)
	var prefix string
	var manager *ManageConfig
	switch this {
	case FilterWorker:
		if cfg.FilterManage == nil {
			return nil, ErrManageCfgEmpty
		}
		prefix = "filter"
		manager = cfg.FilterManage
	case TransferWorker:
		if cfg.TransferManage == nil {
			return nil, ErrManageCfgEmpty
		}
		prefix = "transfer"
		manager = cfg.TransferManage
	case ConsumeWorker:
		if cfg.ConsumeManage == nil {
			return nil, ErrManageCfgEmpty
		}
		prefix = "consumer"
		manager = cfg.ConsumeManage
	default:
		return nil, ErrWorkerTypeNotFound
	}
	for idx, worker := range manager.Workers {
		queueName := fmt.Sprintf("%s_%d_%s_%d-%s", prefix, cfg.Id, manager.Name, idx, worker.TypeName())
		ret = append(ret, queueName)
	}
	return
}

func (this WorkerType) WorkerName(cfg RuleConfigV2, idx int) (ret string, err error) {
	var prefix string
	var manager *ManageConfig
	switch this {
	case FilterWorker:
		prefix = "filter"
		manager = cfg.FilterManage
	case TransferWorker:
		prefix = "transfer"
		manager = cfg.TransferManage
	case ConsumeWorker:
		prefix = "consumer"
		manager = cfg.ConsumeManage
	default:
		err = ErrWorkerTypeNotFound
		return
	}
	if idx >= len(manager.Workers) {
		err = ErrOutOfIndex
		return
	}
	worker := manager.Workers[idx]
	ret = fmt.Sprintf("%s_%d_%s_%d-%s", prefix, cfg.Id, manager.Name, idx, worker.TypeName())
	return
}

func (this WorkerType) Worker(cfg RuleConfigV2, idx int) (ret WorkerConfig, err error) {
	var workers []WorkerConfig
	switch this {
	case FilterWorker:
		workers = cfg.FilterManage.Workers
	case TransferWorker:
		workers = cfg.TransferManage.Workers
	case ConsumeWorker:
		workers = cfg.ConsumeManage.Workers
	default:
		err = ErrWorkerTypeNotFound
		return
	}
	if idx >= len(workers) {
		err = ErrOutOfIndex
		return
	}
	ret = workers[idx]
	return
}

func (this WorkerType) Workers(cfg RuleConfigV2) (ret []WorkerConfig, err error) {
	switch this {
	case FilterWorker:
		ret = cfg.FilterManage.Workers
	case TransferWorker:
		ret = cfg.TransferManage.Workers
	case ConsumeWorker:
		ret = cfg.ConsumeManage.Workers
	default:
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerType) GetLogger(cfg RuleConfigV2) (ret *log.Logger, err error) {
	var log *LogConfig
	if cfg.LogCfg != nil {
		log = cfg.LogCfg
	} else {
		log = DefaultLogCfg
	}
	var name string
	switch this {
	case FilterWorker:
		name = "filter.log"
	case TransferWorker:
		name = "transfer.log"
	case ConsumeWorker:
		name = "consume.log"
	default:
		err = ErrWorkerTypeNotFound
	}
	if name != "" {
		log.SetFilename(fmt.Sprintf("%d-%s", cfg.Id, name))
		return log.GetLogger()
	}
	return
}

const (
	NoWorker WorkerType = iota
	FilterWorker
	TransferWorker
	ConsumeWorker
)

var (
	ErrWorkerTypeNotFound = Err.New("没有发现对应的worker type")
	ErrOutOfIndex         = Err.New("下标越界")
)

type WorkerConfig map[string]interface{}

func (this WorkerConfig) WorkerType() (ret WorkerType, err error) {
	ret = NoWorker
	if _, ok := this["filter_type"]; ok {
		ret = FilterWorker
		return
	}
	if _, ok := this["transfer_type"]; ok {
		ret = TransferWorker
		return
	}
	if _, ok := this["consume_type"]; ok {
		ret = ConsumeWorker
		return
	}
	if ret == NoWorker {
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerConfig) TypeName() (ret string) {
	worker, _ := this.WorkerType()
	return worker.TypeName(this)
}

func (this WorkerConfig) DbRequired() bool {
	_, ok := this["db_required"]
	return ok
}

func (this WorkerConfig) MaxNum() int {
	if num, ok := this["max"]; ok {
		ret := num.(int64)
		return int(ret)
	}
	return 1
}

func (this WorkerConfig) MinNum() int {
	if num, ok := this["min"]; ok {
		ret := num.(int64)
		return int(ret)
	}
	return 1
}

func (this WorkerConfig) IsAggregable() bool {
	_, ok := this["aggregation"]
	return ok
}

func (this WorkerConfig) Aggregator() (*collection.Aggregator, error) {
	if !this.IsAggregable() {
		return nil, ErrAggreNotDefined
	}
	input := this["aggregation"]
	info, ok := input.(map[string]interface{})
	if !ok {
		return nil, ErrAggreNotDefined
	}
	ret, err := collection.CreateByMap(info)
	return ret, err
}

func (this WorkerConfig) HasTableFilter() bool {
	_, ok := this["tablefilter"]
	return ok
}

func (this WorkerConfig) TableFilter() (*TableFilterConfig, error) {
	if !this.HasTableFilter() {
		return nil, ErrTableFilterNotDefined
	}
	info := this["tablefilter"]
	input, ok := info.(map[string]interface{})
	if !ok {
		return nil, ErrTableFilterNotDefined
	}
	return CreateTableFilterByMap(input)
}
