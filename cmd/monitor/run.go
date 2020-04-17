package monitor

import (
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/monitor"
	pbMonitor "github.com/always-waiting/cobra-canal/rpc/pb/monitor"
	servers "github.com/always-waiting/cobra-canal/rpc/servers/monitor"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
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
	port, _ := cmd.Flags().GetString("port")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		panic(err)
	}
	config.LoadV2(cfg)
	obj, err := monitor.MakeMonitor()
	if err != nil {
		panic(err)
	}
	rpc := grpc.NewServer()
	sr := &servers.MonitorRPC{Obj: obj}
	pbMonitor.RegisterManageServer(rpc, sr)
	reflection.Register(rpc)
	done := make(chan bool)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go rpc.Serve(lis)
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
		rpc.Stop()
		done <- true
	}()
	obj.Run()
	<-done
}
