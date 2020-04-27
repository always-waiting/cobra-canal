package collection

import (
	"context"
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"reflect"
	"testing"
	"time"
)

func createEvent(table string, id string) event.EventV2 {
	return event.EventV2{
		Table: &event.Table{
			Schema: "db", Name: table,
			Columns: []string{"id"}, //[]schema.TableColumn{{Name: "id", Type: schema.TYPE_STRING}},
		},
		RawData: [][]interface{}{[]interface{}{id}},
	}
}

func TestAggregator_01(t *testing.T) {
	idxRulesCfg := []IdxRuleConfig{
		{Tables: []string{"t_device_basic", "t_device_config"}, IdxField: "id"},
	}
	gatherMap := make(map[string]Indexer)
	for _, idx := range idxRulesCfg {
		tables := idx.Tables
		for _, table := range tables {
			gatherMap[table] = idx
		}
	}
	duration, _ := time.ParseDuration("3s")
	ctx, cancel := context.WithCancel(context.Background())
	aggre := Aggregator{
		gatherMap: gatherMap,
		Interval:  duration,
		keyChan:   make(chan string, 0),
		pool:      make(map[string]Element),
		timerList: make(map[string]*time.Timer),
		Ctx:       ctx,
		cancel:    cancel,
	}
	out := aggre.Collection()
	count := 0
	done := make(chan struct{}, 0)
	go func() {
		for {
			select {
			case <-out:
				count++
			case <-aggre.Ctx.Done():
				done <- struct{}{}
				return

			}
		}
	}()
	max := 70000
	idx := 0
	for idx < max {
		aggre.Add(createEvent("t_device_basic", fmt.Sprintf("%d", idx)))
		idx++
	}
	for {
		fmt.Println(count)
		time.Sleep(10 * time.Second)
		if count == 70000 {
			aggre.Close()
			break
		}
	}
	fmt.Println(count)
}

func TestAggregator_00(t *testing.T) {
	idxRulesCfg := []IdxRuleConfig{
		{Tables: []string{"t_device_basic", "t_device_config"}, IdxField: "id"},
	}
	gatherMap := make(map[string]Indexer)
	for _, idx := range idxRulesCfg {
		tables := idx.Tables
		for _, table := range tables {
			gatherMap[table] = idx
		}
	}
	duration, _ := time.ParseDuration("3s")
	ctx, cancel := context.WithCancel(context.Background())
	aggre := Aggregator{
		gatherMap: gatherMap,
		Interval:  duration,
		keyChan:   make(chan string, 0),
		pool:      make(map[string]Element),
		timerList: make(map[string]*time.Timer),
		Ctx:       ctx,
		cancel:    cancel,
	}
	out := aggre.Collection()
	result := make([]interface{}, 0)
	expected := make([]interface{}, 0)
	done := make(chan struct{}, 0)
	go func() {
		for {
			select {
			case ele := <-out:
				switch ele.Key {
				case "e4-0", "e4-1", "e4-2", "e4-3", "e4-4":
					rest := result[3].(map[string]Element)
					rest[ele.Key] = ele
					result[3] = rest
				case "e3-0":
					rest := result[2].([]Element)
					rest = append(rest, ele)
					result[2] = rest
				case "e2-0":
					rest := result[1].([]Element)
					rest = append(rest, ele)
					result[1] = rest
				case "e1-0":
					rest := result[0].([]Element)
					rest = append(rest, ele)
					result[0] = rest
				default:
					result = append(result, ele)
				}
			case <-aggre.Ctx.Done():
				done <- struct{}{}
				return
			}
		}
	}()
	{ // flush聚合消费
		result = append(result, []Element{})
		aggre.Add(createEvent("t_device_basic", "e1-0"))
		aggre.Add(createEvent("t_device_basic", "e1-0"))
		aggre.Flush()
		expected = append(expected, []Element{
			{
				Key:    "e1-0",
				Events: []event.EventV2{createEvent("t_device_basic", "e1-0"), createEvent("t_device_basic", "e1-0")},
			},
		})
	}
	{ // 聚合逻辑
		result = append(result, []Element{})
		aggre.Add(createEvent("t_device_basic", "e2-0"))
		aggre.Flush()
		aggre.Add(createEvent("t_device_config", "e2-0"))
		aggre.Flush()
		expected = append(expected, []Element{
			{
				Key:    "e2-0",
				Events: []event.EventV2{createEvent("t_device_basic", "e2-0")},
			}, {
				Key:    "e2-0",
				Events: []event.EventV2{createEvent("t_device_config", "e2-0")},
			},
		})
	}
	{ // 正常3秒聚合
		result = append(result, []Element{})
		aggre.Add(createEvent("t_device_basic", "e3-0"))
		aggre.Add(createEvent("t_device_config", "e3-0"))
		time.Sleep(duration)
		expected = append(expected, []Element{
			{
				Key:    "e3-0",
				Events: []event.EventV2{createEvent("t_device_basic", "e3-0"), createEvent("t_device_config", "e3-0")},
			},
		})
	}
	{ // 写入后立刻退出
		result = append(result, map[string]Element{})
		aggre.Add(createEvent("t_device_basic", "e4-0"))
		aggre.Add(createEvent("t_device_config", "e4-1"))
		aggre.Add(createEvent("t_device_config", "e4-2"))
		aggre.Add(createEvent("t_device_config", "e4-3"))
		aggre.Add(createEvent("t_device_config", "e4-4"))
		aggre.Close()
		expected = append(expected, map[string]Element{
			"e4-0": {
				Key:    "e4-0",
				Events: []event.EventV2{createEvent("t_device_basic", "e4-0")},
			},
			"e4-1": Element{
				Key:    "e4-1",
				Events: []event.EventV2{createEvent("t_device_config", "e4-1")},
			},
			"e4-2": Element{
				Key:    "e4-2",
				Events: []event.EventV2{createEvent("t_device_config", "e4-2")},
			},
			"e4-3": Element{
				Key:    "e4-3",
				Events: []event.EventV2{createEvent("t_device_config", "e4-3")},
			},
			"e4-4": Element{
				Key:    "e4-4",
				Events: []event.EventV2{createEvent("t_device_config", "e4-4")},
			},
		})
	}
	<-done
	//if !reflect.DeepEqual(result[0].Events, expected[0].Events) {
	for idx, res := range result {
		if len(expected)-1 < idx {
			t.Errorf("第%d个结果不符合预期: got(%#v), expeceted(nil)", idx, res)
			continue
		}
		if !reflect.DeepEqual(res, expected[idx]) {
			t.Errorf("第%d个结果不符合预期: got(%#v), expected(%#v)", idx, res, expected[idx])
		}
	}
}
