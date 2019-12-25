package errors

import (
	"errors"
	"fmt"
)

const (
	BUFFER = 10
)

func MakeErrHandler(sender Sender, buffer int) *ErrHandler {
	errh := ErrHandler{}
	errh.sender = sender
	errh.errChannel = make(chan error, buffer)
	errh.confirm = make(chan bool, 1)
	return &errh
}

func MakeFakeHandler() *ErrHandler {
	errh := ErrHandler{}
	errh.sender = FakeSender{}
	errh.errChannel = make(chan error, 10)
	errh.confirm = make(chan bool, 1)
	return &errh
}

type Sender interface {
	Send(string) (string, error)
}

type ErrHandler struct {
	sender     Sender
	errChannel chan error
	closed     bool
	isSendable bool
	confirm    chan bool
}

func (this *ErrHandler) Reset() {
	this.errChannel = make(chan error, cap(this.errChannel))
}

func (this *ErrHandler) Push(input interface{}) {
	if this.closed {
		return
	}
	err, ok := input.(error)
	if !ok {
		err = errors.New(fmt.Sprintf("%v", input))
	}
	this.errChannel <- err
}

func (this *ErrHandler) Send() {
	if this.isSendable {
		return
	}
	this.isSendable = true
	for {
		err, isOpen := <-this.errChannel
		if !isOpen {
			break
		}
		this.sender.Send(err.Error())
	}
	this.confirm <- true
}

func (this *ErrHandler) Close() {
	if this.closed {
		return
	}
	close(this.errChannel)
	this.closed = true
	<-this.confirm
}

type FakeSender struct{}

func (this FakeSender) Send(doc string) (string, error) {
	fmt.Println(">>>>fake error sender<<<<")
	fmt.Println(doc)
	return "", nil
}
