package monitor

import (
	"github.com/always-waiting/cobra-canal/config"
	"testing"
)

func TestMontorV2(t *testing.T) {
	if cfgMark == Cfg00 {
		testMonitor_Cfg00_1(t)
	}
}

func testMonitor_Cfg00_1(t *testing.T) {
	config.LoadTestCfg("../examples/config/00-example.toml")
	cobra, err := MakeMonitor()
	if err != nil {
		t.Errorf("生成cobra对象出错: %s", err)
	}
	go func() {
		cobra.Run()

	}()
	if _, err := cobra.SavePosition(); err != nil {
		t.Errorf("SavePosition出错: %s", err)
	}
	cobra.Close()
}
