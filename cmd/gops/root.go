package gops

import (
	"fmt"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "gops",
	Short:   "对接gops命令",
	Version: "1.0.0",
	Run:     rootCmdRun,
}

const (
	SUCCESS1 = "%s successfully\n"
)

func init() {
	RootCmd.AddCommand(startDebugCmd)
	RootCmd.AddCommand(stopDebugCmd)
}

func rootCmdRun(cmd *cobra.Command, args []string) {
	pid, _ := cmd.Flags().GetString("pid")
	if pid == "" {
		panic("pid不能为空")
	}
	exitStatus, output, err := helps.RunCommand("gops", true, pid)
	if exitStatus != 0 || err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "gops")
	if output != "" {
		fmt.Println(output)
	}
}
