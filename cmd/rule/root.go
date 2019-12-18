package rule

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use:     "rule",
	Short:   "对正在同步的程序提供规则行为改变",
	Version: "1.0.0",
}

func init() {
	RootCmd.PersistentFlags().String("port", "", "程序监控的端口号")
	RootCmd.PersistentFlags().String("pid", "", "程序的pid号")
	RootCmd.PersistentFlags().String("rule", "", "规则名称")
	RootCmd.MarkPersistentFlagRequired("rule")
	RootCmd.AddCommand(stopCmd)
	RootCmd.AddCommand(startCmd)
}

const (
	ERR1 = "port和pid必须提供一个"
	ERR2 = "没有发现监听的端口"
)
