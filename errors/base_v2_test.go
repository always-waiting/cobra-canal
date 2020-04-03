package errors

import (
	baseErr "errors"
	"fmt"
	"testing"
)

type testSender struct {
	context []string
}

func (this *testSender) Send(doc string) (string, error) {
	this.context = append(this.context, doc)
	return "", nil
}

func TestFake(t *testing.T) {
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
