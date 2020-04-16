package config

import (
	Err "errors"
	"fmt"
	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-log/log"
)

type RuleConfigV2 struct {
	Id             int                     `toml:"id"`
	Desc           string                  `toml:"desc"`
	QueueAddr      string                  `toml:"queue_addr"`
	LogCfg         LogConfig               `toml:"log"`
	DbCfg          *MysqlConfig            `toml:"db"`
	ErrCfg         errors.ErrHandlerConfig `toml:"err"`
	FilterManage   ManageConfig            `toml:"filtermanage"`
	TransferManage ManageConfig            `toml:"transfermanage"`
	ConsumeManage  ManageConfig            `toml:"consumemanage"`
	Compress       bool                    `toml:"compress"`
}

func (this RuleConfigV2) IsAggreable() bool {
	return this.FilterManage.IsAggreable()
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

func (this RuleConfigV2) Aggregator() (*collection.Aggregator, error) {
	return this.FilterManage.Aggregator()
}

func (this RuleConfigV2) HasTableFilter() bool {
	return this.FilterManage.HasTableFilter()
}

func (this RuleConfigV2) TableFilter() *TableFilterConfig {
	return this.FilterManage.TableFilterCfg

}

func (this RuleConfigV2) ErrHandler() errors.ErrHandlerV2 {
	return this.ErrCfg.MakeHandler()
}

type ManageConfig struct {
	Name           string                  `toml:"name"`
	Desc           string                  `toml:"desc"`
	Port           int                     `toml:"port"`
	DbRequired     bool                    `toml:"db_required"`
	Worker         WorkerConfig            `toml:"worker"`
	TableFilterCfg *TableFilterConfig      `toml:"tablefilter"`
	AggreCfg       *collection.AggreConfig `toml:"aggregation"`
	Workers        []WorkerConfig          `toml:"workers"`
}

func (this *ManageConfig) HasTableFilter() bool {
	return this.TableFilterCfg != nil
}

func (this *ManageConfig) IsAggreable() bool {
	return this.AggreCfg != nil
}

func (this *ManageConfig) Aggregator() (*collection.Aggregator, error) {
	return collection.CreateAggregator(this.AggreCfg)
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
	switch this {
	case FilterWorker:
		name := fmt.Sprintf("filter_%d_%s_%s", cfg.Id, cfg.FilterManage.Name, cfg.FilterManage.Worker.TypeName())
		ret = append(ret, name)
	case TransferWorker:
		for idx, transfer := range cfg.TransferManage.Workers {
			queueName := fmt.Sprintf("transfer_%d_%s_%d-%s", cfg.Id, cfg.TransferManage.Name, idx, transfer.TypeName())
			ret = append(ret, queueName)
		}
	case ConsumeWorker:
		for idx, consume := range cfg.ConsumeManage.Workers {
			name := fmt.Sprintf("consume_%d_%s_%d-%s", cfg.Id, cfg.ConsumeManage.Name, idx, consume.TypeName())
			ret = append(ret, name)
		}
	default:
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerType) WorkerName(cfg RuleConfigV2, idx int) (ret string, err error) {
	switch this {
	case FilterWorker:
		ret = fmt.Sprintf("filter_%d_%s_%s", cfg.Id, cfg.FilterManage.Name, cfg.FilterManage.Worker.TypeName())
	case TransferWorker:
		workers := cfg.TransferManage.Workers
		if idx >= len(workers) {
			err = ErrOutOfIndex
			return
		}
		for index, transfer := range cfg.TransferManage.Workers {
			if index == idx {
				ret = fmt.Sprintf("transfer_%d_%s_%d-%s", cfg.Id, cfg.TransferManage.Name, idx, transfer.TypeName())
				break
			}
		}
	case ConsumeWorker:
		workers := cfg.ConsumeManage.Workers
		if idx >= len(workers) {
			err = ErrOutOfIndex
			return
		}
		for index, consume := range cfg.ConsumeManage.Workers {
			if index == idx {
				ret = fmt.Sprintf("consume_%d_%s_%d-%s", cfg.Id, cfg.ConsumeManage.Name, idx, consume.TypeName())
				break
			}
		}
	default:
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerType) Worker(cfg RuleConfigV2, idx int) (ret WorkerConfig, err error) {
	switch this {
	case FilterWorker:
		ret = cfg.FilterManage.Worker
	case TransferWorker:
		workers := cfg.TransferManage.Workers
		if idx >= len(workers) {
			err = ErrOutOfIndex
			return
		}
		for i, worker := range workers {
			if i == idx {
				ret = worker
				break
			}
		}
	case ConsumeWorker:
		workers := cfg.ConsumeManage.Workers
		if idx >= len(workers) {
			err = ErrOutOfIndex
			return
		}
		for i, worker := range workers {
			if i == idx {
				ret = worker
				break
			}
		}
	default:
		err = ErrWorkerTypeNotFound
	}
	return
}

func (this WorkerType) Workers(cfg RuleConfigV2) (ret []WorkerConfig, err error) {
	switch this {
	case FilterWorker:
		ret = []WorkerConfig{cfg.FilterManage.Worker}
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
	log := cfg.LogCfg
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
