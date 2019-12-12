package helps

import (
	"fmt"
	"regexp"
)

func GetPidByServiceName(name string) (pid string, err error) {
	exitStatus, output, err := RunCommand("systemctl", true, "status", name)
	if exitStatus != 0 {
		err = fmt.Errorf(ERR6, name)
		return
	}
	if err != nil {
		err = fmt.Errorf(ERR7, err)
		return
	}
	reg1 := regexp.MustCompile(PID_MATCH_STRING)
	outputByte := []byte(output)
	if !reg1.Match(outputByte) {
		err = fmt.Errorf(ERR8, PID_MATCH_STRING)
		return
	}
	matchGroup := reg1.FindSubmatch(outputByte)
	pid = string(matchGroup[1])
	return
}
