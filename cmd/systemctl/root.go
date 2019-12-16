package systemctl

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:     "systemctl",
	Short:   "对接systemctl命令",
	Version: "1.0.0",
}

const (
	SERVICE_PREFIX = "cobra."
	SUCCESS1       = "%s successfully\n"
)

func init() {
	RootCmd.PersistentFlags().String("service", "", "服务名称,会自动添加"+SERVICE_PREFIX+"前缀")
	RootCmd.MarkPersistentFlagRequired("service")
	RootCmd.AddCommand(installCmd)
	RootCmd.AddCommand(uninstallCmd)
	RootCmd.AddCommand(startCmd)
	RootCmd.AddCommand(stopCmd)
	RootCmd.AddCommand(restartCmd)
	RootCmd.AddCommand(statusCmd)
}
