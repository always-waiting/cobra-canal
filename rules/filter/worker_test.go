package filter

import (
	"github.com/always-waiting/cobra-canal/event"
	"testing"
)

func TestWorker(t *testing.T) {
	if cfgMark == Cfg00 {
		testWorker_Cfg00_1(t)
	}
}

// 只有1可以通过
func testRuler1(e *event.EventV2) (bool, error) {
	id, err := e.GetFloat(0, "id")
	if err != nil {
		return false, err
	}
	if id == 1 {
		return true, err
	}
	return false, err
}

// 只有2可以通过
func testRuler2(e *event.EventV2) (bool, error) {
	id, err := e.GetFloat(0, "id")
	if err != nil {
		return false, err
	}
	if id == 2 {
		return true, err
	}
	return false, err
}

func testWorker_Cfg00_1(t *testing.T) {
	AddFilterRuler("base", testRuler1)
	testManager_Cfg00_2(t)
	AddFilterRuler("base", testRuler2)
	testManager_Cfg00_2(t)
}
