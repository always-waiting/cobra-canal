package cmd

import (
	mcobra "github.com/always-waiting/cobra-canal/cobra"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/google/gops/agent"
	"github.com/siddontang/go-log/log"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var runCmd = &cobra.Command{
	Use:     "run",
	Short:   "直接开启监控",
	Long:    `可以通过此命令直接开启监控程序`,
	Version: "1.0.0",
	Run:     runCmdRun,
}

func init() {
}

func runCmdRun(cmd *cobra.Command, args []string) {
	if len(args) == 0 {
		panic("需要输入配置文件")
	}
	cfg := args[0]
	config.Load(cfg)
	log.Info("生成binlog监控器")
	log.Infof("配置文件为:%s", cfg)
	var err error
	cobraMonitor, err := mcobra.MakeCobra()
	if err != nil {
		panic(err)
	}
	log.Info("binlog监控器生成完毕")
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)
	go func() {
	LOOP1:
		for {
			select {
			case signalGet := <-sigs:
				switch signalGet {
				case syscall.SIGINT, syscall.SIGTERM:
					log.Info("开始关闭监控程序")
					agent.Close()
					break LOOP1
				case syscall.SIGUSR1:
					log.Info("开启debug")
					agent.Listen(agent.Options{})
				case syscall.SIGUSR2:
					log.Info("关闭debug")
					agent.Close()
				default:
					log.Info(signalGet)
				}
			}
		}
		cobraMonitor.Close()
		done <- true
	}()
	err = cobraMonitor.Run()
	if err != nil {
		log.Errorf("运行出错信息:%s", err)
		sigs <- syscall.SIGTERM
	}
	<-done

}
