package helps

import (
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"syscall"
)

const (
	ERR1             = "%q failed to connect stdout pipe: %v"
	ERR2             = "%q failed to connect stderr pipe: %v"
	ERR3             = "%q failed: %v"
	ERR4             = "%q failed while attempting to read stdout: %v"
	ERR5             = "%q failed with stderr: %s"
	ERR6             = "service(%s) not exists"
	ERR7             = "未知错误(%v)"
	ERR8             = "不能匹配到信息(%s)"
	ERR9             = "port和pid必须提供一个"
	ERR10            = "没有发现监听的端口"
	PID_MATCH_STRING = `Main PID: (\d+)`
)

func RunCommand(command string, readStdout bool, arguments ...string) (int, string, error) {
	cmd := exec.Command(command, arguments...)
	var output string
	var stdout io.ReadCloser
	var err error
	if readStdout {
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return 0, "", fmt.Errorf(ERR1, command, err)
		}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return 0, "", fmt.Errorf(ERR2, command, err)
	}
	if err := cmd.Start(); err != nil {
		return 0, "", fmt.Errorf(ERR3, command, err)
	}
	if command == "launchctl" {
		slurp, _ := ioutil.ReadAll(stderr)
		if len(slurp) > 0 {
			return 0, "", fmt.Errorf(ERR5, command, slurp)
		}
	}
	if readStdout {
		out, err := ioutil.ReadAll(stdout)
		if err != nil {
			return 0, "", fmt.Errorf(ERR4, command, err)
		} else if len(out) > 0 {
			output = string(out)
		}
	}
	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				return status.ExitStatus(), output, err
			}
		}
		return 0, output, fmt.Errorf(ERR3, command, err)
	}
	return 0, output, nil
}
