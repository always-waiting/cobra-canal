package consume

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"testing"
	"time"
)

func TestWorker(t *testing.T) {
	if cfgMark == Cfg00 {
		testWorker_Cfg00_1(t)
		testWorker_Cfg00_2(t)
	}
}

func testWorker_Cfg00_1(t *testing.T) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManagerWithNext(ruleCfg)
	if err != nil {
		t.Errorf("创建transfer manager失败: %s", err)
	}
	{
		for i := 0; i < 3; i++ {
			manager.Push([]byte(fmt.Sprintf("消费池内容-%d", i)))
		}
	}
	go func() { manager.Start() }()
	time.Sleep(5 * time.Second)
	manager.Close()
}

func testRuler1(data []byte) error {
	fmt.Printf("从消费池获取的数据为: %#v\n", data)
	return nil
}

func testWorker_Cfg00_2(t *testing.T) {
	AddConsumeRuler("test", testRuler1)
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	ruleCfg.ConsumeManage.Workers[0] = map[string]interface{}{"consume_type": "test"}
	manager, err := CreateManagerWithNext(ruleCfg)
	if err != nil {
		t.Errorf("创建consume manager失败: %s", err)
	}
	{
		for i := 0; i < 2; i++ {
			manager.Push([]byte(fmt.Sprintf("变更消费逻辑-%d", i)))
		}
	}
	go func() { manager.Start() }()
	time.Sleep(5 * time.Second)
	manager.Close()
}
