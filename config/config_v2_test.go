package config

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-mysql/canal"
)

func TestLoadConfigV2_00(t *testing.T) {
	path, err := filepath.Abs("./")
	if err != nil {
		t.Errorf("获取目录地址失败:%s", err)
		t.Skip()
	}
	file := fmt.Sprintf("%s/%s", path, "../examples/config/00-example.toml")
	LoadV2(file)
	cfg := ConfigV2()
	{
		if cfg.path != file {
			t.Errorf("Error for path, got(%s), expected(%s)", cfg.path, file)
		}
	}
	{
		cobraCfg := &CobraConfig{
			Config: &canal.Config{Addr: "localhost:3306", User: "root", Password: "abc123", ServerID: 90000000, IncludeTableRegex: []string{"db_cmdb\\..*"}},
			DbCfg:  &MysqlConfig{Addr: "localhost:3306", User: "root", Passwd: "abc123", Db: "db_cmdb_cobra"},
			LogCfg: LogConfig{Type: "file", Level: "debug", Dirname: "/export/Logs/cobra/"},
			ErrCfg: errors.ErrHandlerConfig(map[string]string{"type": "fake"}),
			Rebase: true,
		}
		if !reflect.DeepEqual(cobraCfg, cfg.CobraCfg) {
			t.Errorf("cobra配置不同")
		}
	}
}
