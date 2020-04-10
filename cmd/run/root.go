package run

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "run",
	Short:   "各个组件的启动命令",
	Version: "2.0.0",
}

func init() {
	RootCmd.PersistentFlags().String("cfg", "", "配置文件")
	RootCmd.AddCommand(monitorCmd)
	RootCmd.AddCommand(filterCmd)
}
