package config

import (
	"github.com/siddontang/go-mysql/canal"
)

type configure struct {
	CanalCfg     *canal.Config   `toml:"canal" description:"canal配置"`
	MysqlCfg     *MysqlConfig    `toml:"mysql" description:"眼镜蛇mysql库配置"`
	LogCfg       LogConfig       `toml:"log" description:"眼镜蛇日志配置"`
	BufferNum    int             `toml:"buffer_number" description:"事件缓存个数"`
	Port         int             `toml:"port" description:"程序对外的交互端口"`
	RulesCfg     []RuleConfig    `toml:"rules" description:"规则配置"`
	ErrSenderCfg errSenderConfig `toml:"err_sender" description:"错误处理配置"`
	RebaseFlag   bool            `toml:"rebase" description:"是否重新定位监控点"`
	path         string          `description:"配置文件路径, 如果没有为空"`
}

type AggreConfig struct {
	Type        string          `toml:"type" description:"类型"`
	Time        int             `toml:"time" description:"缓存秒数"`
	IdxRulesCfg []IdxRuleConfig `toml:"idxrule" description:"缓存键的生成规则"`
}

type IdxRuleConfig struct {
	Tables       []string `toml:"tables" description:"记录日志的表"`
	IdxField     string   `toml:"idx_field" description:"日志唯一key字段"`
	IdxPrefix    string   `toml:"idx_prefix" description:"日志唯一key前缀"`
	IdxType      string   `toml:"idx_type" description:"日志唯一key字段类型"`
	AggreField   string   `toml:"aggre_field" description:"用于多表关联的聚合域"`
	UserField    string   `toml:"user_field" description:"操作人字段"`
	PrimaryKey   string   `toml:"primary_key" description:"表主键字段"`
	ExcludeField []string `toml:"exclude_field" description:"表主键字段"`
}
