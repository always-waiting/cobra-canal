package rabbitmq

import (
	"testing"
	"time"
)

const (
	TEST_ADDR = "amqp://guest:guest@localhost:5672/cobra"
)

func TestSess_00(t *testing.T) {
	sess, err := New("test", TEST_ADDR, []string{"test_queue1"})
	if err != nil {
		t.Errorf("生成session错误%s", err)
	}
	time.Sleep(1 * time.Second)
	err = sess.Push([]byte("this is an test"))
	if err != nil {
		t.Errorf("发送错误%s", err)
	}
}
