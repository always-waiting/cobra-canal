package filter

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
	"sync"
	"testing"
	"time"
)

func createTestWorker() (*Worker, error) {
	cfg := config.ConfigV2()
	ruleCfg := cfg.RulesCfg[0]
	manager, err := CreateManager(ruleCfg)
	if err != nil {
		return nil, err
	}
	worker, err := CreateWorker(manager)
	return worker, err
}

func TestWorker(t *testing.T) {
	if cfgMark == Cfg00 {
		testWorker_Cfg00_1(t)
		testWorker_Cfg00_2(t)
	}
}

func testWorker_Cfg00_1(t *testing.T) {
	worker, err := createTestWorker()
	if err != nil {
		t.Errorf("创建worker失败: %s", err)
	}
	{
		flag := make(chan bool, 0)
		e := event.EventV2{}
		go func() {
			defer func() { flag <- true }()
			if !worker.Analyze(e) {
				t.Errorf("base过滤规则没有正常运行")
			}
		}()
		<-flag
	}
	worker.Release()
}

func testWorker_Cfg00_2(t *testing.T) {
	worker, err := createTestWorker()
	if err != nil {
		t.Errorf("创建worker失败: %s", err)
	}
	{
		ruler := func(e *event.EventV2) (bool, error) {
			id, err := e.GetInt(0, "id")
			if err != nil {
				return false, err
			}
			time.Sleep(5 * time.Second)
			if id%2 == 0 {
				return true, nil
			}
			return false, nil
		}
		worker.rules = []FilterRuler{ruler}
		wg := sync.WaitGroup{}
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				e := event.EventV2{
					Table:   &event.Table{Columns: []string{"id"}},
					RawData: [][]interface{}{[]interface{}{id}},
				}
				if id%2 == 0 {
					if !worker.Analyze(e) {
						t.Errorf("过滤规则没有返回正确结果, got(false), expected(true)")
					}
				} else {
					if worker.Analyze(e) {
						t.Errorf("过滤规则没有返回正确结果, got(true), expected(false)")
					}
				}
			}(i)
		}
		wg.Wait()
	}
	worker.Release()
}
