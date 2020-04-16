package monitor

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/monitor"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "监控组件启动命令",
	Version: "2.0.0",
	Run:     runCmdRun,
}

func runCmdRun(cmd *cobra.Command, args []string) {
	cfg, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfg)
	obj, err := monitor.MakeMonitor()
	if err != nil {
		panic(err)
	}
	done := make(chan bool)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
	LOOP1:
		for {
			select {
			case signalGet := <-sigs:
				switch signalGet {
				case syscall.SIGINT, syscall.SIGTERM:
					break LOOP1
				default:
					fmt.Println(signalGet)
				}
			}
		}
		obj.Close()
		done <- true
	}()
	obj.Run()
	<-done
}
