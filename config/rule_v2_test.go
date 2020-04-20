package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/always-waiting/cobra-canal/collection"
	"github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
)

func TestLoadConfigV2_Rule_00(t *testing.T) {
	path, err := filepath.Abs("./")
	if err != nil {
		t.Errorf("获取目录地址失败:%s", err)
		t.Skip()
	}
	file := fmt.Sprintf("%s/%s", path, "../examples/config/00-example.toml")
	LoadV2(file)
	cfg := ConfigV2()
	errCfg := errors.ErrHandlerConfig(map[string]string{"type": "fake"})
	wCfg := WorkerConfig(map[string]interface{}{
		"filter_type": "base", "max": int64(10),
		"aggregation": map[string]interface{}{
			"time": int64(10),
			"idxrule": []interface{}{
				map[string]interface{}{
					"tables":        []interface{}{"t_device_basic"},
					"idx_field":     "id",
					"exclude_field": []interface{}{"action_id"},
				},
			},
		},
	})
	{
		rulesCfg := []RuleConfigV2{
			{
				Id: 1, Desc: "工厂简单描述", QueueAddr: "amqp://guest:guest@localhost:5672/cobra",
				LogCfg: &LogConfig{Type: "file", Level: "debug", Dirname: "/export/Logs/cobra"},
				DbCfg:  &MysqlConfig{Addr: "addr", User: "user", Passwd: "passwd", Db: "db"},
				ErrCfg: &errCfg,
				FilterManage: &ManageConfig{
					Name: "filtername", Desc: "说明",
					TableFilterCfg: &TableFilterConfig{DbName: "db_cmdb", Include: []string{"t_device_basic", "t_device_config"}},
					Workers:        []WorkerConfig{wCfg},
				},
				TransferManage: &ManageConfig{
					Name: "transfername", Desc: "说明",
					Workers: []WorkerConfig{
						map[string]interface{}{"transfer_type": "base"},
						map[string]interface{}{"transfer_type": "base"},
					},
				},
				ConsumeManage: &ManageConfig{
					Name: "consumename", Desc: "说明",
					Workers: []WorkerConfig{WorkerConfig(map[string]interface{}{"consume_type": "base"})},
				},
			},
		}
		if !reflect.DeepEqual(rulesCfg, cfg.RulesCfg) {
			t.Errorf("%#v", rulesCfg[0].FilterManage.Workers)
			t.Errorf("%#v", cfg.RulesCfg[0].FilterManage.Workers)
			t.Errorf("rules配置不同")
		}
	}
}

func TestIdxRuleConfig_00(t *testing.T) {
	idxRuleCfg1 := collection.IdxRuleConfig{
		Tables:   []string{"t_device_basic", "t_device_config"},
		IdxField: "id",
	}
	e := event.EventV2{
		Table: &event.Table{
			Schema: "db_namn", Name: "t_device_basic",
			Columns: []string{"id"},
		},
		RawData: [][]interface{}{[]interface{}{1}},
	}

	ret, err := idxRuleCfg1.Idx(e)
	if err != nil {
		t.Errorf("解析聚合键值失败: %s", err)
	}
	if ret != "1" {
		t.Errorf("聚合键值获取错误: got(%s), expected(%s)", ret, "1")
	}

}

func TestWorkerConfig_00(t *testing.T) {
	path, err := filepath.Abs("./")
	if err != nil {
		t.Errorf("获取目录地址失败:%s", err)
		t.Skip()
	}
	file := fmt.Sprintf("%s/%s", path, "../examples/config/00-example.toml")
	LoadV2(file)
	cfg := ConfigV2()
	rule := cfg.RulesCfg[0]
	worker := rule.FilterManage.Workers[0]
	worker.Aggregator()
}
