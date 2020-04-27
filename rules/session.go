package rules

import (
	"sync"
)

type Session struct {
	chans      []chan interface{}
	queuenames []string
	buffer     int
}

func NewSession(buffer int, names ...string) *Session {
	ret := &Session{queuenames: names, buffer: buffer}
	chans := make([]chan interface{}, 0)
	for i := 0; i < len(names); i++ {
		chans = append(chans, make(chan interface{}, buffer))
	}
	ret.chans = chans
	return ret
}

func (this *Session) QueueNames() []string {
	return this.queuenames
}

func (this *Session) Push(info interface{}, ids ...int) error {
	wg := sync.WaitGroup{}
	if len(ids) == 0 {
		wg.Add(len(this.chans))
		for i := 0; i < len(this.chans); i++ {
			go func(idx int) {
				defer wg.Done()
				this.chans[idx] <- info
			}(i)
		}
	} else {
		for _, i := range ids {
			if i >= len(this.chans) {
				continue
			}
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				this.chans[idx] <- info
			}(i)
		}
	}
	wg.Wait()
	return nil
}

func (this *Session) PushByIdx(idx int, info interface{}) error {
	if idx >= len(this.chans) {
		return ErrOutOfIndex
	}
	this.chans[idx] <- info
	return nil
}

func (this *Session) StreamAll() ([]chan interface{}, error) {
	return this.chans, nil
}

func (this *Session) Close() {}
