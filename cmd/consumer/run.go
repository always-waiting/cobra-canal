package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/always-waiting/cobra-canal/config"

	"github.com/always-waiting/cobra-canal/rules/consume"
	"github.com/spf13/cobra"

	"sync"
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "启动转换数据流程",
	Version: "2.0.0",
	Run:     runCmdRun,
}

func runCmdRun(cmd *cobra.Command, args []string) {
	cfgFile, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfgFile)
	cfg := config.ConfigV2()
	rulesCfg := cfg.RulesCfg
	managers := make([]*consume.Manager, 0)
	for _, ruleCfg := range rulesCfg {
		manager, err := consume.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		managers = append(managers, manager)
	}
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
		for _, manager := range managers {
			go manager.Close()
		}
	}()
	wg := sync.WaitGroup{}
	for _, manager := range managers {
		wg.Add(1)
		go func(m *consume.Manager) {
			defer func() { wg.Done() }()
			m.Start()
		}(manager)
	}
	wg.Wait()
}
