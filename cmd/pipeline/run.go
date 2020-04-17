package pipeline

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/monitor"
	pbConsumer "github.com/always-waiting/cobra-canal/rpc/pb/consumer"
	pbFilter "github.com/always-waiting/cobra-canal/rpc/pb/filter"
	pbMonitor "github.com/always-waiting/cobra-canal/rpc/pb/monitor"
	pbTransfer "github.com/always-waiting/cobra-canal/rpc/pb/transfer"
	consumerServers "github.com/always-waiting/cobra-canal/rpc/servers/consumer"
	filterServers "github.com/always-waiting/cobra-canal/rpc/servers/filter"
	monitorServers "github.com/always-waiting/cobra-canal/rpc/servers/monitor"
	transferServers "github.com/always-waiting/cobra-canal/rpc/servers/transfer"
	"github.com/always-waiting/cobra-canal/rules/consumer"
	"github.com/always-waiting/cobra-canal/rules/filter"
	"github.com/always-waiting/cobra-canal/rules/transfer"
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
	Short:   "启动流程",
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
	monitorObj, err := monitor.MakeMonitor()
	if err != nil {
		panic(err)
	}
	rpc := grpc.NewServer()
	mSr := &monitorServers.MonitorRPC{Obj: monitorObj}

	fMs := make([]*filter.Manager, 0)
	fSr := &filterServers.ManagerRPC{Obj: make(map[int64]*filter.Manager)}

	tSr := &transferServers.ManagerRPC{Obj: make(map[int64]*transfer.Manager)}
	tMs := make([]*transfer.Manager, 0)

	cMs := make([]*consumer.Manager, 0)
	cSr := &consumerServers.ManagerRPC{Obj: make(map[int64]*consumer.Manager)}
	for _, ruleCfg := range rulesCfg {
		fM, err := filter.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		fid, _ := fM.Id()
		fSr.Obj[fid] = fM
		fMs = append(fMs, fM)

		tM, err := transfer.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		tid, _ := tM.Id()
		tSr.Obj[tid] = tM
		tMs = append(tMs, tM)

		cM, err := consumer.CreateManagerWithNext(ruleCfg)
		if err != nil {
			panic(err)
		}
		mid, _ := cM.Id()
		cSr.Obj[mid] = cM
		cMs = append(cMs, cM)
	}
	pbMonitor.RegisterManageServer(rpc, mSr)
	pbFilter.RegisterManageServer(rpc, fSr)
	pbTransfer.RegisterManageServer(rpc, tSr)
	pbConsumer.RegisterManageServer(rpc, cSr)
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
		monitorObj.Close()
		for _, m := range fMs {
			m.Close()
		}
		for _, m := range tMs {
			m.Close()
		}
		for _, m := range cMs {
			m.Close()
		}
		rpc.Stop()
	}()
	go func() {
		wg.Add(1)
		rpc.Serve(lis)
	}()
	go monitorObj.Run()
	for _, m := range fMs {
		wg.Add(1)
		go func(m *filter.Manager) {
			defer func() { wg.Done() }()
			m.Start()
		}(m)
	}
	for _, m := range tMs {
		wg.Add(1)
		go func(m *transfer.Manager) {
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
	wg.Wait()

}
