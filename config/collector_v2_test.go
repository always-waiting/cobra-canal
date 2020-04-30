package config

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/event"
	"testing"
	"time"
)

func TestCollectorV2_1(t *testing.T) {
	ag := &AggreConfig{Time: 10}
	c := makeCollectorV2(ag)
	c.Start()
	stop := make(chan bool)
	go func() {
		count := 0
		for {
			select {
			case es := <-c.SendChan:
				typs := []string{}
				for _, e := range es {
					typs = append(typs, e.Type)
				}
				t.Logf("收到信息:%#v\n", typs)
				count++
			}
			if count == 100 {
				stop <- true
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		c.AddEvent(fmt.Sprintf("sn%d", i), event.Event{Type: fmt.Sprintf("sn%d", i)})
	}
	time.Sleep(time.Second)
	for i := 0; i < 100; i++ {
		c.AddEvent(fmt.Sprintf("sn%d", i), event.Event{Type: fmt.Sprintf("sn%d", i)})
	}
	<-stop
}
