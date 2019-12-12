package systemctl

import (
	"fmt"
	"github.com/kardianos/service"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:     "start",
	Short:   "systemctl start的alias",
	Version: "1.0.0",
	Run:     startCmdRun,
}

func startCmdRun(cmd *cobra.Command, args []string) {
	name, _ := cmd.Flags().GetString("service")
	name = SERVICE_PREFIX + name
	svcConfig := &service.Config{
		Name: name,
	}
	prg := &Program{}
	var s service.Service
	var err error
	if s, err = service.New(prg, svcConfig); err != nil {
		panic(err)
	}
	if err = service.Control(s, "start"); err != nil {
		panic(err)
	}
	fmt.Printf(SUCCESS1, "start")
}
