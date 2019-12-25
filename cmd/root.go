package cmd

import (
	"github.com/always-waiting/cobra-canal/cmd/gops"
	"github.com/always-waiting/cobra-canal/cmd/rule"
	"github.com/always-waiting/cobra-canal/cmd/systemctl"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "cmdb_cobra",
	Short: "mysql监控命令组",
	Long: `进行mysql的binlog监控，可以根据不同需要，开发不同的
过滤规则以及下游消费`,
}

func Execute() {
	rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(systemctl.RootCmd)
	rootCmd.AddCommand(rule.RootCmd)
	rootCmd.AddCommand(gops.RootCmd)
	rootCmd.AddCommand(runCmd)
}

const (
	SUCCESS1       = "%s successfully\n"
	SERVICE_PREFIX = "cobra."
)
