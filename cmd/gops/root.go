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
	SERVICE_PREFIX = "cobra."
	SUCCESS1       = "%s successfully\n"
)

func init() {
	RootCmd.PersistentFlags().String("service", "", "服务名称,会自动添加"+SERVICE_PREFIX+"前缀")
	RootCmd.PersistentFlags().String("pid", "", "相应查看的pid号")
	//RootCmd.MarkPersistentFlagRequired("service")
	RootCmd.AddCommand(startDebugCmd)
	RootCmd.AddCommand(stopDebugCmd)
}

func rootCmdRun(cmd *cobra.Command, args []string) {
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
	exitStatus, output, err := helps.RunCommand("gops", true, pid)
	if exitStatus != 0 || err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "gops")
	if output != "" {
		fmt.Println(output)
	}
}
