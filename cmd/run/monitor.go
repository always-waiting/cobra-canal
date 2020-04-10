package run

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/monitor"
	"github.com/spf13/cobra"
)

var monitorCmd = &cobra.Command{
	Use:     "monitor",
	Short:   "启动binlog日志监控程序",
	Version: "2.0.0",
	Run:     monitorCmdRun,
}

func monitorCmdRun(cmd *cobra.Command, args []string) {
	cfg, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfg)
	obj, err := monitor.MakeMonitor()
	if err != nil {
		panic(err)
	}
	obj.Run()
}
