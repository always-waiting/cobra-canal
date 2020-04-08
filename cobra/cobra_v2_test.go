package cobra

import (
	"github.com/always-waiting/cobra-canal/config"
	"testing"
)

func TestCobraV2_00(t *testing.T) {
	config.LoadTestCfg("../examples/config/00-example.toml")
	cobra, err := MakeCobraV2()
	if err != nil {
		t.Errorf("生成cobra对象出错: %s", err)
	}
	exitFlag := make(chan bool, 0)
	go func() {
		cobra.Run()
		exitFlag <- true

	}()
	if _, err := cobra.SavePosition(); err != nil {
		t.Errorf("SavePosition出错: %s", err)
	}
	cobra.Close()
	<-exitFlag
}
