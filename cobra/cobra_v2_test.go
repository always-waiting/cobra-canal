package cobra

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"path/filepath"
	"testing"
)

func TestCobraV2_00(t *testing.T) {
	path, err := filepath.Abs("./")
	if err != nil {
		t.Errorf("获取目录地址失败:%s", err)
		t.Skip()
	}
	file := fmt.Sprintf("%s/%s", path, "../examples/config/00-example.toml")
	config.LoadV2(file)
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
