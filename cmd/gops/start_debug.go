package gops

import (
	"fmt"

	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var startDebugCmd = &cobra.Command{
	Use:     "startDebug",
	Short:   "开启debug模式",
	Version: "1.0.0",
	Run:     startDebugCmdRun,
}

func startDebugCmdRun(cmd *cobra.Command, args []string) {
	pid, _ := cmd.Flags().GetString("pid")
	if pid == "" {
		name, _ := cmd.Flags().GetString("service")
		name = SERVICE_PREFIX + name
		var err error
		pid, err = helps.GetPidByServiceName(name)
		if err != nil {
			panic(err)
		}
	}
	exitStatus, output, err := helps.RunCommand("kill", true, "-10", pid)
	if exitStatus != 0 || err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "startDebug")
	if output != "" {
		fmt.Println(output)
	}

}
