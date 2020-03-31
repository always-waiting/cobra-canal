package rule

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "rule",
	Short:   "对正在同步的程序提供规则行为改变",
	Version: "1.0.0",
}

func init() {
	RootCmd.AddCommand(stopCmd)
	RootCmd.AddCommand(startCmd)
	RootCmd.AddCommand(reportCmd)
	RootCmd.AddCommand(listCmd)
}
