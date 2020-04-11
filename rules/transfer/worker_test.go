package transfer

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
	"testing"
	"time"
)

func TestWorker(t *testing.T) {
	if cfgMark == Cfg00 {
		testWorker_Cfg00_1(t)
		testWorker_Cfg00_2(t)
	}
}

func testRuler1(es []event.EventV2) (interface{}, error) {
	return fmt.Sprintf(`{"事件个数":%d}`, len(es)), nil
}

func testWorker_Cfg00_1(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManagerWithNext(ruleCfg)
	if err != nil {
		t.Errorf("创建transfer manager失败: %s", err)
	}
	{
		e1, _ := event.FromJSON([]byte(EXAMPLE1))
		e2, _ := event.FromJSON([]byte(EXAMPLE2))
		e3, _ := event.FromJSON([]byte(EXAMPLE3))
		es := []event.EventV2{e1[0], e2[0], e3[0]}
		for i := 0; i < 2; i++ {
			manager.Push(es)
		}
	}
	go func() { manager.Start() }()
	time.Sleep(5 * time.Second)
	manager.Close()
}

func testWorker_Cfg00_2(t *testing.T) {
	AddTransferRuler("test", testRuler1)
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	ruleCfg.TransferManage.Workers[0] = map[string]interface{}{"consume_type": "test"}
	manager, err := CreateManagerWithNext(ruleCfg)
	if err != nil {
		t.Errorf("创建transfer manager失败: %s", err)
	}
	{
		e1, _ := event.FromJSON([]byte(EXAMPLE1))
		e2, _ := event.FromJSON([]byte(EXAMPLE2))
		e3, _ := event.FromJSON([]byte(EXAMPLE3))
		es := []event.EventV2{e1[0], e2[0], e3[0]}
		for i := 0; i < 2; i++ {
			manager.Push(es)
		}
	}
	go func() { manager.Start() }()
	time.Sleep(5 * time.Second)
	manager.Close()
}
