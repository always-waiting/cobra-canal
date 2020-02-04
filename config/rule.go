package config

type RuleConfig struct {
	Name           string             `toml:"name" description:"规则的名称"`
	Desc           string             `toml:"desc" description:"规则的简介"`
	BufferNum      int                `toml:"buffer_number" description:"事件缓存个数"`
	ReplySync      []string           `toml:"reply_sync" description:"规则需要相应的同步类型"`
	MasterDBCfg    *MysqlConfig       `toml:"masterdb" description:"上游监控数据库名，用于生成读取上游数据库的对象"`
	TableFilterCfg *TableFilterConfig `toml:"tablefilter" description:"表过滤器"`
	ConsumerCfg    []*ConsumerConfig  `toml:"consumer" description:"消费对象配置"`
	AggreCfg       *AggreConfig       `toml:"aggregation" description:"缓存配置"`
	ErrSenderCfg   errSenderConfig    `toml:"err_sender"`
	LogCfg         LogConfig          `toml:"log"`
	WorkerNum      int                `toml:"worker"`
}

func (r *RuleConfig) HasTableFilter() bool {
	return r.TableFilterCfg != nil
}

func (r *RuleConfig) IsAggreable() bool {
	return r.AggreCfg != nil
}

func (r *RuleConfig) InitAggregator() (ret Aggregatable) {
	if r.IsAggreable() {
		aggre := makeDefaultAggregator(r)
		ret = aggre
	}
	return
}

func (r *RuleConfig) GetBufferNum() int {
	if r.BufferNum == 0 {
		return 10
	}
	return r.BufferNum
}

func (r *RuleConfig) Worker() int {
	if r.WorkerNum == 0 {
		return 1
	}
	return r.WorkerNum
}
