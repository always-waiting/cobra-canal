package positions

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "position",
	Short:   "mysql同步位置信息处理逻辑",
	Version: "1.0.0",
}

func init() {
	RootCmd.AddCommand(reportCmd)
	RootCmd.AddCommand(saveCmd)
}
