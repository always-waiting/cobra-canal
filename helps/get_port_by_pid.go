package helps

import (
	"errors"
	"io/ioutil"
	"os/exec"
	"regexp"

	"github.com/spf13/cobra"
)

func GetPortByPid(pid string) (ret string, err error) {
	if pid == "" {
		err = errors.New("pid不能为空")
		return
	}
	cmds := make([]*exec.Cmd, 0)
	cmds = append(cmds, exec.Command("netstat", "-nltp"))
	cmds = append(cmds, exec.Command("grep", pid))
	for i := 1; i < len(cmds); i++ {
		cmds[i].Stdin, _ = cmds[i-1].StdoutPipe()
	}
	out, _ := cmds[len(cmds)-1].StdoutPipe()
	var get []byte
	for i := 1; i < len(cmds); i++ {
		if err = cmds[i].Start(); err != nil {
			return
		}
	}
	cmds[0].Run()
	for i := 1; i < len(cmds); i++ {
		if i == len(cmds)-1 {
			if get, err = ioutil.ReadAll(out); err != nil {
				return
			}
		}
		if err = cmds[i].Wait(); err != nil {
			return
		}
	}
	if len(get) == 0 {
		return
	}
	reg := regexp.MustCompile(`\d{3}\.0\.0\.1:(\d+)`)
	matches := reg.FindAllSubmatch(get, -1)
	if len(matches) == 0 {
		return
	}
	ret = string(matches[len(matches)-1][1])
	return
}

func GetPort(cmd *cobra.Command) (port string, err error) {
	port, _ = cmd.Flags().GetString("port")
	if port == "" {
		pid, _ := cmd.Flags().GetString("pid")
		if pid == "" {
			err = errors.New(ERR9)
			return
		}
		if port, err = GetPortByPid(pid); err != nil {
			return
		}
		if port == "" {
			err = errors.New(ERR10)
			return
		}
	}
	return
}
