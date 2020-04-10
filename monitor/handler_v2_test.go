package monitor

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
		Cfg00: "../examples/config/00-example.toml",
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

func TestHandlerV2(t *testing.T) {
	if cfgMark == Cfg00 {
		testHandlerV2_Cfg00_1(t)
	}

}

func testHandlerV2_Cfg00_1(t *testing.T) {
	cfg := config.ConfigV2()
	c := &Monitor{
		cfg: cfg,
	}
	h, err := CreateHandlerV2(c)
	if err != nil {
		t.Errorf("生成handler对象出错: %s", err)
	}
	t.Log(h.String())
}
