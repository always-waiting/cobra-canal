package errors

import (
	baseErr "errors"
	"fmt"
	"testing"
	"time"
)

type testSender struct {
	context []string
}

func (this *testSender) Send(doc string) (string, error) {
	this.context = append(this.context, doc)
	return "", nil
}

func TestFake_00(t *testing.T) {
	tSender := &testSender{}
	errH := ErrHandlerV2{sender: tSender}
	errH.Init()
	go errH.Send()
	mails := []string{"a", "b", "c", "d", "e"}
	for _, mail := range mails {
		errH.Push(baseErr.New(fmt.Sprintf("%s", mail)))
	}
	for idx, mail := range tSender.context {
		fmt.Println(mail)
		if mail != mails[idx] {
			t.Errorf("第%d个信息不准确, got(%s), expected(%s)", idx, mail, mails[idx])
		}

	}
	errH.Close()
}

func TestFake_01(t *testing.T) {
	tSender := &testSender{}
	errH := ErrHandlerV2{sender: tSender}
	errH.Init()
	go errH.Send()
	mails := make([]int, 1000)
	for _, mail := range mails {
		go errH.Push(baseErr.New(fmt.Sprintf("info - %d", mail)))
	}
	time.Sleep(1 * time.Nanosecond)
	errH.Close()
	t.Logf("处理了%d个错误", len(tSender.context))
}
