package consume

import (
	"flag"
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"os"
	"testing"
)

const (
	Cfg00 = "00"
)

var (
	confMap = map[string]string{
		Cfg00: "../../examples/config/00-example.toml",
	}
	cfgMark string
)

func TestMain(m *testing.M) {
	flag.StringVar(&cfgMark, "cfgmark", "", "配置文件标记")
	flag.Parse()
	if cfgMark == "" {
		fmt.Println("输入配置文件标记")
		os.Exit(1)
	}
	filename, ok := confMap[cfgMark]
	if !ok {
		fmt.Printf("没有定义配置文件标记%s\n", cfgMark)
		os.Exit(1)
	}
	err := config.LoadTestCfg(filename)
	if err != nil {
		fmt.Printf("配置加载错误: %s\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestManager(t *testing.T) {
	if cfgMark == Cfg00 {
		testManager_Cfg00_1(t)
		testManager_Cfg00_2(t)
	}
}

func testManager_Cfg00_1(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManager(ruleCfg)
	if err != nil {
		t.Errorf("创建consume manager失败: %s", err)
	}
	if name, err := manager.Name(); err != nil {
		t.Errorf("consume manager解析名称失败")
	} else {
		if name != "consume_1_consumename" {
			t.Errorf("consume manager的名字解析失败, got(%s), expected(%s)", name, "consume_1_consumename")
		}
	}
}

func testManager_Cfg00_2(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManager(ruleCfg)
	if err != nil {
		t.Errorf("创建consume manager失败: %s", err)
	}
	{
		for i := 0; i < 2; i++ {
			manager.Push(i)
		}
	}
	manager.Close()
}
