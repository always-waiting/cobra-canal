package errors

import (
	baseErr "errors"
	"fmt"
	"testing"
)

func TestSend(t *testing.T) {
	// t.Fatal("not implemented")
	sender := FakeSender{}
	errH := MakeErrHandler(sender, 10)
	go errH.Send()
	for i := 0; i < 5; i++ {
		errH.Push(baseErr.New(fmt.Sprintf("测试%d", i)))
	}
	errH.Close()
}
