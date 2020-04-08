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
	FilterManage   FilterManageConfig      `toml:"filtermanage"`
	TransferManage TransferManageConfig    `toml:"transfermanage"`
	ConsumeManage  ConsumeManageConfig     `toml:"consumemanage"`
	Port           int                     `toml:"port"`
	Host           string                  `toml:"host"`
	Compress       bool                    `toml:"compress"`
}

func (this RuleConfigV2) IsAggreable() bool {
	return this.FilterManage.IsAggreable()
}

func (this RuleConfigV2) FilterManagerName() string {
	return fmt.Sprintf("filter_%d_%s", this.Id, this.FilterManage.Name)
}

func (this RuleConfigV2) FilterWorker() WorkerConfig {
	return this.FilterManage.Worker
}

func (this RuleConfigV2) TransferWorker() []WorkerConfig {
	return this.TransferManage.Workers
}

func (this RuleConfigV2) FilterWorkerName() string {
	return fmt.Sprintf("filter_%d_%s_%s", this.Id, this.FilterManage.Name, this.FilterManage.Worker.TypeName())
}

func (this RuleConfigV2) TransferManagerName() string {
	return fmt.Sprintf("transfer_%d_%s", this.Id, this.TransferManage.Name)
}

func (this RuleConfigV2) TransferWorkerName() []string {
	ret := make([]string, 0)
	for idx, transfer := range this.TransferManage.Workers {
		queueName := fmt.Sprintf("transfer_%d_%s_%d-%s", this.Id, this.TransferManage.Name, idx, transfer.TypeName())
		ret = append(ret, queueName)
	}
	return ret
}

func (this RuleConfigV2) GetFilterManagerLogger() (logger *log.Logger, err error) {
	log := this.LogCfg
	log.SetFilename(fmt.Sprintf("%d-filtermanager.log", this.Id))
	return log.GetLogger()
}

func (this RuleConfigV2) GetLogger(name string) (logger *log.Logger, err error) {
	log := this.LogCfg
	log.SetFilename(fmt.Sprintf("%d-%s", this.Id, name))
	return log.GetLogger()
}

func (this RuleConfigV2) Aggregator() (*collection.Aggregator, error) {
	return this.FilterManage.Aggregator()
}

type FilterManageConfig struct {
	Name           string                  `toml:"name"`
	Desc           string                  `toml:"desc"`
	Percent        int                     `toml:"percent"`
	DbRequired     bool                    `toml:"db_required"`
	Worker         WorkerConfig            `toml:"worker"`
	TableFilterCfg *TableFilterConfig      `toml:"tablefilter"`
	AggreCfg       *collection.AggreConfig `toml:"aggregation"`
}

func (this *FilterManageConfig) HasTableFilter() bool {
	return this.TableFilterCfg != nil
}

func (this *FilterManageConfig) IsAggreable() bool {
	return this.AggreCfg != nil
}

func (this *FilterManageConfig) Aggregator() (*collection.Aggregator, error) {
	return collection.CreateAggregator(this.AggreCfg)

}

type TransferManageConfig struct {
	Name       string         `toml:"name"`
	Desc       string         `toml:"desc"`
	Percent    int            `toml:"percent"`
	DbRequired bool           `toml:"db_required"`
	Workers    []WorkerConfig `toml:"workers"`
}

type ConsumeManageConfig struct {
	Name       string         `toml:"name"`
	Desc       string         `toml:"desc"`
	Percent    int            `toml:"percent"`
	DbRequired bool           `toml:"db_required"`
	Workers    []WorkerConfig `toml:"workers"`
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

const (
	NoWorker WorkerType = iota
	FilterWorker
	TransferWorker
	ConsumeWorker
)

var (
	ErrWorkerTypeNotFound = Err.New("没有发现对应的worker type")
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
