package run

import (
	"github.com/always-waiting/cobra-canal/config"

	"github.com/always-waiting/cobra-canal/rules/transfer"
	"github.com/spf13/cobra"

	"sync"
)

var transferCmd = &cobra.Command{
	Use:     "transfer",
	Short:   "启动转换数据流程",
	Version: "2.0.0",
	Run:     transferCmdRun,
}

func transferCmdRun(cmd *cobra.Command, args []string) {
	cfgFile, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfgFile)
	cfg := config.ConfigV2()
	rulesCfg := cfg.RulesCfg
	managers := make([]*transfer.Manager, 0)
	for _, ruleCfg := range rulesCfg {
		manager, err := transfer.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		managers = append(managers, manager)
	}
	wg := sync.WaitGroup{}
	for _, manager := range managers {
		wg.Add(1)
		go func(m *transfer.Manager) {
			defer func() { wg.Done() }()
			m.Start()
		}(manager)
	}
	wg.Wait()
}
