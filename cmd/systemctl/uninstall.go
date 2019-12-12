package systemctl

import (
	"fmt"
	"github.com/kardianos/service"
	"github.com/spf13/cobra"
)

var uninstallCmd = &cobra.Command{
	Use:     "uninstall",
	Short:   "卸载systemctl的安装",
	Long:    "根据输入，卸载systemctl注册的监控",
	Version: "1.0.0",
	Run:     uninstallCmdRun,
}

func uninstallCmdRun(cmd *cobra.Command, args []string) {
	name, _ := cmd.Flags().GetString("service")
	name = SERVICE_PREFIX + name
	svcConfig := &service.Config{
		Name: name,
	}
	prg := &Program{}
	var s service.Service
	var err error
	if s, err = service.New(prg, svcConfig); err != nil {
		panic(err)
	}
	if err = service.Control(s, "uninstall"); err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "uninstall")
}
