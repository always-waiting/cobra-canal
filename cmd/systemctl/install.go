package systemctl

import (
	"fmt"
	"github.com/kardianos/service"
	"github.com/spf13/cobra"
)

var installCmd = &cobra.Command{
	Use:     "install",
	Short:   "对监控任务进行systemctl安装",
	Long:    `对监控任务进行开启启动安装，可以直接使用systemctl命令进行维护`,
	Version: "1.0.0",
	Run:     installCmdRun,
}

func init() {
	installCmd.Flags().String("config", "", "配置文件地址，需要绝对路径")
	installCmd.Flags().String("description", "", "服务的简要说明")
	installCmd.MarkFlagRequired("config")
}

func installCmdRun(cmd *cobra.Command, args []string) {
	options := service.KeyValue{
		"ReloadSignal": "30",
	}
	name, _ := cmd.Flags().GetString("service")
	name = SERVICE_PREFIX + name
	desc, _ := cmd.Flags().GetString("description")
	config, _ := cmd.Flags().GetString("config")

	svcConfig := &service.Config{
		Name:        name,
		DisplayName: name,
		Description: desc,
		Arguments:   []string{"run", config},
		Option:      options,
	}
	prg := &Program{}
	var s service.Service
	var err error
	if s, err = service.New(prg, svcConfig); err != nil {
		panic(err)
	}
	if err = service.Control(s, "install"); err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "install")
}
