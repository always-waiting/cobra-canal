package pipeline

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "pipeline",
	Short:   "组合运行命令集",
	Version: "2.0.0",
}

func init() {
	RootCmd.PersistentFlags().String("cfg", "", "配置文件")
	RootCmd.PersistentFlags().Int64("id", 0, "规则id")
	RootCmd.AddCommand(runCmd)
	//RootCmd.AddCommand(getCfgCmd)
}
