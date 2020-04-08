package errors

import (
	"context"
	"errors"
	"fmt"
)

func New(info string) error {
	return errors.New(info)
}

func Errorf(format string, args ...interface{}) error {
	return errors.New(fmt.Sprintf(format, args...))
}

type Sender interface {
	Send(string) (string, error)
}

type ErrHandlerV2 struct {
	sender     Sender
	ctx        context.Context
	cancal     context.CancelFunc
	errChannel chan error
}

func (this *ErrHandlerV2) SetSender(s Sender) {
	this.sender = s
}

func (this *ErrHandlerV2) Init() {
	this.errChannel = make(chan error, cap(this.errChannel))
	this.ctx, this.cancal = context.WithCancel(context.Background())
}

func (this *ErrHandlerV2) Push(input interface{}) {
	if this.errChannel != nil {
		err, ok := input.(error)
		if !ok {
			err = errors.New(fmt.Sprintf("%v", input))
		}
		this.errChannel <- err
	}
}

func (this *ErrHandlerV2) Send() {
	for {
		select {
		case <-this.ctx.Done():
			this.errChannel = nil
			return
		case err := <-this.errChannel:
			this.sender.Send(err.Error())
		}
	}
}

func (this *ErrHandlerV2) Close() {
	this.cancal()
}

type FakeSender struct{}

func (this FakeSender) Send(doc string) (string, error) {
	fmt.Println(">>>>fake error sender<<<<")
	fmt.Println(doc)
	return doc, nil
}
