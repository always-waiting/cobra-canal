package rule

import (
	"errors"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "rule",
	Short:   "对正在同步的程序提供规则行为改变",
	Version: "1.0.0",
}

func init() {
	RootCmd.PersistentFlags().String("port", "", "程序监控的端口号")
	RootCmd.PersistentFlags().String("pid", "", "程序的pid号")
	RootCmd.AddCommand(stopCmd)
	RootCmd.AddCommand(startCmd)
	RootCmd.AddCommand(reportCmd)
}

const (
	ERR1 = "port和pid必须提供一个"
	ERR2 = "没有发现监听的端口"
)

func getPort(cmd *cobra.Command) (port string, err error) {
	port, _ = cmd.Flags().GetString("port")
	if port == "" {
		pid, _ := cmd.Flags().GetString("pid")
		if pid == "" {
			err = errors.New(ERR1)
			return
		}
		if port, err = helps.GetPortByPid(pid); err != nil {
			return
		}
		if port == "" {
			err = errors.New(ERR2)
			return
		}
	}
	return

}
