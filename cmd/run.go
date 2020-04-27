package cmd

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
	Short:   "启动流程",
	Version: "2.0.0",
	Run:     runCmdRun,
}

func init() {

}

func runCmdRun(cmd *cobra.Command, args []string) {
	cfgFile, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfgFile)
	monitorObj, err := monitor.MakeMonitor()
	if err != nil {
		panic(err)
	}
	/*
		fMs := make([]*filter.Manager, 0)

		cMs := make([]*consumer.Manager, 0)
		for _, ruleCfg := range rulesCfg {
			fM, err := filter.CreateManagerWithNext(ruleCfg)
			if err != nil {
				panic(err)
			}
			fMs = append(fMs, fM)

			cM, err := consumer.CreateManagerWithNext(ruleCfg)
			if err != nil {
				panic(err)
			}
			cMs = append(cMs, cM)
		}
	*/
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
		monitorObj.Close()
		/*
			for _, m := range fMs {
				m.Close()
			}
			for _, m := range cMs {
				m.Close()
			}
		*/
	}()
	monitorObj.Run()
	/*
		for _, m := range fMs {
			wg.Add(1)
			go func(m *filter.Manager) {
				defer func() { wg.Done() }()
				m.Start()
			}(m)
		}
		for _, m := range cMs {
			wg.Add(1)
			go func(m *consumer.Manager) {
				defer func() { wg.Done() }()
				m.Start()
			}(m)
		}
	*/

}
