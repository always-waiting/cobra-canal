package helps

import (
	"io/ioutil"
	"os/exec"
	"regexp"
)

func GetPortByPid(pid string) (ret string, err error) {
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
