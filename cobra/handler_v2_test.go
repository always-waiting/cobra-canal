package cobra

import (
	"github.com/always-waiting/cobra-canal/config"
	"testing"
)

func TestHandlerV2_00(t *testing.T) {
	cfg := config.ConfigV2()
	c := &CobraV2{
		cfg: cfg,
	}
	h, err := CreateHandlerV2(c)
	if err != nil {
		t.Errorf("生成handler对象出错: %s", err)
	}
	t.Log(h.String())
}
