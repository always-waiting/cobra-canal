package config

type ConsumerConfig struct {
	BufferNum    int             `toml:"buffer_number" description:"事件缓存个数"`
	LogCfg       LogConfig       `toml:"log"`
	ErrSenderCfg errSenderConfig `toml:"err_sender" description:"错误处理配置"`
	Type         string          `toml:"type" description:"下游消费类型"`
	Db           string          `toml:"db"`
	User         string          `toml:"user"`
	Passwd       string          `toml:"passwd"`
	Addr         string          `toml:"addr"`
	Url          string          `toml:"url"`
	App          string          `toml:"app"`
	Key          string          `toml:"key"`
	Id           int             `toml:"id"`
	Route        string          `toml:"route"`
	Mandatory    bool            `toml:"mandatory"`
	Immediate    bool            `toml:"immediate"`
	SuccessField string          `toml:"success_field"`
	SuccessCode  int             `toml:"success_code"`
	Method       string          `toml:"method"`
	ChannelCfg   *ChannelConfig  `toml:"channel"`
	QueueCfg     *QueueConfig    `toml:"queue"`
	WorkerNum    int             `toml:"worker"`
	Desc         string          `toml:"desc"`
}

func (c *ConsumerConfig) GetBufferNum() int {
	if c.BufferNum == 0 {
		return 10
	}
	return c.BufferNum
}

func (c *ConsumerConfig) Worker() int {
	if c.WorkerNum == 0 {
		return 1
	}
	return c.WorkerNum
}

type ChannelConfig struct {
	Name        string `toml:"name"`
	Type        string `toml:"type"`
	Durable     bool   `toml:"durable"`
	AutoDeleted bool   `toml:"auto_deleted"`
	Internal    bool   `toml:"internal"`
	NoWait      bool   `toml:"no_wait"`
}

type QueueConfig struct {
	Name      string                 `toml:"name"`
	Durable   bool                   `toml:"durable"`
	Delete    bool                   `toml:"delete"`
	Exclusive bool                   `toml:"exclusive"`
	NoWait    bool                   `toml:"no_wait"`
	Args      map[string]interface{} `toml:"args"`
}

func (this *ConsumerConfig) ToMysqlConfig() (cfg MysqlConfig) {
	cfg = MysqlConfig{
		Addr:   this.Addr,
		Passwd: this.Passwd,
		Db:     this.Db,
		User:   this.User,
	}
	return cfg
}
