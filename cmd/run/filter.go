package run

import (
	"github.com/always-waiting/cobra-canal/config"

	"github.com/always-waiting/cobra-canal/rules/filter"
	"github.com/spf13/cobra"

	"sync"
)

var filterCmd = &cobra.Command{
	Use:     "filter",
	Short:   "启动binlog日志监控程序",
	Version: "2.0.0",
	Run:     filterCmdRun,
}

func filterCmdRun(cmd *cobra.Command, args []string) {
	cfgFile, _ := cmd.Flags().GetString("cfg")
	config.LoadV2(cfgFile)
	cfg := config.ConfigV2()
	rulesCfg := cfg.RulesCfg
	managers := make([]*filter.Manager, 0)
	for _, ruleCfg := range rulesCfg {
		manager, err := filter.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		managers = append(managers, manager)
	}
	wg := sync.WaitGroup{}
	for _, manager := range managers {
		wg.Add(1)
		go func(m *filter.Manager) {
			defer func() { wg.Done() }()
			m.Start()
		}(manager)

	}
	wg.Wait()
}
