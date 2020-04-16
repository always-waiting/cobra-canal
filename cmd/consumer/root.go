package consumer

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "consumer",
	Short:   "消费组件命令集",
	Version: "2.0.0",
}

func init() {
	RootCmd.PersistentFlags().String("cfg", "", "配置文件")
	RootCmd.AddCommand(runCmd)
}
