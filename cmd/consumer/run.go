package consumer

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	pb "github.com/always-waiting/cobra-canal/rpc/pb/consumer"
	servers "github.com/always-waiting/cobra-canal/rpc/servers/consumer"
	"github.com/always-waiting/cobra-canal/rules/consumer"
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
	Short:   "启动转换数据流程",
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
	managers := make([]*consumer.Manager, 0)
	rpc := grpc.NewServer()
	sr := &servers.ManagerRPC{Obj: make(map[int64]*consumer.Manager)}
	for _, ruleCfg := range rulesCfg {
		manager, err := consumer.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		id, _ := manager.Id()
		sr.Obj[id] = manager
		managers = append(managers, manager)
	}
	pb.RegisterManageServer(rpc, sr)
	reflection.Register(rpc)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	wg := sync.WaitGroup{}
	go func() {
		defer func() { wg.Done() }()
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
	go func() {
		wg.Add(1)
		rpc.Serve(lis)
	}()
	for _, manager := range managers {
		wg.Add(1)
		go func(m *consumer.Manager) {
			defer func() { wg.Done() }()
			m.Start()
		}(manager)
	}
	wg.Wait()
}
