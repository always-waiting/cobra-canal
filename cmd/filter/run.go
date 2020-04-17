package filter

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/rules/filter"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "启动过滤流程",
	Version: "2.0.0",
	Run:     runCmdRun,
}

func runCmdRun(cmd *cobra.Command, args []string) {
	cfgFile, _ := cmd.Flags().GetString("cfg")
	port, _ := cmd.Flags().GetString("port")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}
	config.LoadV2(cfgFile)
	cfg := config.ConfigV2()
	rulesCfg := cfg.RulesCfg
	managers := make([]*filter.Manager, 0)
	rpc := grpc.NewServer()
	sr := &filter.ManagerRPC{Obj: make(map[int64]*filter.Manager)}
	for _, ruleCfg := range rulesCfg {
		manager, err := filter.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		id, _ := manager.Id()
		sr.Obj[id] = manager
		managers = append(managers, manager)
	}
	filter.RegisterFilterServer(rpc, sr)
	reflection.Register(rpc)
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
		rpc.Stop()
	}()
	go rpc.Serve(lis)
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
