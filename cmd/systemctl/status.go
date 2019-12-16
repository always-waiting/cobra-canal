package systemctl

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/helps"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:     "status",
	Short:   "systemctl status的alias",
	Version: "1.0.0",
	Run:     statusCmdRun,
}

func statusCmdRun(cmd *cobra.Command, args []string) {
	name, _ := cmd.Flags().GetString("service")
	name = SERVICE_PREFIX + name
	_, output, _ := helps.RunCommand("systemctl", true, "status", name)
	fmt.Print(output)
}
