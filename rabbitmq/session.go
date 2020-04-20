package rabbitmq

import (
	"errors"
	"github.com/siddontang/go-log/log"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected    = errors.New("not connected to a server")
	errAlreadyClosed   = errors.New("already closed: not connected to the server")
	errShutdown        = errors.New("session is shutting down")
	errOutOfIdx        = errors.New("下标越界")
	errQueueNotDefined = errors.New("队列名称没定义")
)

func makeQueueExist(list []string) func(string) bool {
	cache := make(map[string]struct{})
	for _, name := range list {
		cache[name] = struct{}{}
	}
	return func(name string) bool {
		_, ok := cache[name]
		return ok
	}
}

type Session struct {
	queuenames      []string
	exchangename    string
	Log             *log.Logger
	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	isReady         bool
	ReChanSignal    chan bool
	isQueueExist    func(string) bool
}

func (this *Session) QueueNames() []string {
	return this.queuenames
}

func (this *Session) Chan() *amqp.Channel {
	return this.channel
}

func New(exName, addr string, eqName ...string) (*Session, error) {
	session := &Session{
		queuenames:   eqName,
		exchangename: exName,
		done:         make(chan bool),
		//isQueueExist: makeQueueExist(eqName),
	}
	go session.handleReconnect(addr)
	count := 0
	for !session.isReady && count < 20 {
		count++
		time.Sleep(reInitDelay)
	}
	if !session.isReady {
		return session, errNotConnected
	}
	return session, nil
}

func (this *Session) IsReady() bool {
	return this.isReady
}

func (this *Session) Debug(data string) {
	if this.Log != nil {
		this.Log.Debug(data)
	}
}

func (this *Session) Debugf(data string, params ...interface{}) {
	if this.Log != nil {
		this.Log.Debugf(data, params...)
	}
}

func (this *Session) handleReconnect(addr string) {
	for {
		this.isReady = false
		this.Debug("Attempting to connect")
		conn, err := this.connect(addr)
		if err != nil {
			this.Debug("Failed to connect. Retrying...")
			select {
			case <-this.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		if done := this.handleReInit(conn); done {
			break
		}
	}
}

func (this *Session) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	this.changeConnection(conn)
	this.Debug("Connected!")
	return conn, nil
}

func (this *Session) changeConnection(connection *amqp.Connection) {
	this.connection = connection
	this.notifyConnClose = make(chan *amqp.Error)
	this.connection.NotifyClose(this.notifyConnClose)
}

func (this *Session) handleReInit(conn *amqp.Connection) bool {
	for {
		this.isReady = false
		err := this.init(conn)
		if err != nil {
			this.Debug("Failed to initialize channel. Retrying...")
			select {
			case <-this.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}
		select {
		case <-this.done:
			return true
		case <-this.notifyConnClose:
			this.Debug("Connection closed. Reconnecting...")
			return false
		case <-this.notifyChanClose:
			this.Debug("Channel closed. Re-running init...")
		}
	}
}

func (this *Session) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	err = ch.Confirm(false)
	if err != nil {
		return err
	}
	if err = ch.ExchangeDeclare(
		this.exchangename, amqp.ExchangeTopic, true, false, false, false, nil,
	); err != nil {
		return err
	}
	for _, queueName := range this.queuenames {
		if queue, err := ch.QueueDeclare(
			queueName, true, false, false, false, nil,
		); err != nil {
			return err
		} else {
			if err = ch.QueueBind(
				queue.Name, queue.Name, this.exchangename, false, nil,
			); err != nil {
				return err
			}
		}
	}
	if err != nil {
		return err
	}
	this.changeChannel(ch)
	this.isReady = true
	this.Debug("Setup!")
	return nil
}

func (this *Session) changeChannel(channel *amqp.Channel) {
	this.channel = channel
	this.notifyChanClose = make(chan *amqp.Error)
	this.notifyConfirm = make(chan amqp.Confirmation, 100)
	this.channel.NotifyClose(this.notifyChanClose)
	this.channel.NotifyPublish(this.notifyConfirm)
	if this.ReChanSignal != nil {
		go func() { this.ReChanSignal <- true }()
	}
}

func (this *Session) Close() error {
	if !this.isReady {
		return errAlreadyClosed
	}
	close(this.done)
	done := make(chan struct{}, 0)
	var err error
	go func() {
		defer func() { done <- struct{}{} }()
		if err = this.channel.Close(); err != nil {
			return
		}
		err = this.connection.Close()
	}()
	select {
	case <-done:
	case <-time.After(reInitDelay):
	}
	this.isReady = false
	return nil
}

func (this *Session) Push(data []byte, ids ...int) error {
	if !this.isReady {
		return errors.New("failed to push push: not connected")
	}
	wg := sync.WaitGroup{}
	if len(ids) == 0 {
		for idx, _ := range this.queuenames {
			ids = append(ids, idx)
		}
	}
	for _, id := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			this.PushByIdx(i, data)
		}(id)
	}
	wg.Wait()
	return nil
}

func (this *Session) PushByIdx(idx int, data []byte) error {
	if len(this.queuenames) <= idx {
		return errQueueNotDefined
	}
	queuename := this.queuenames[idx]
LOOP2:
	for {
		err := this.UnsafePush(data, queuename)
		if err != nil {
			this.Debugf("队列%d: Push failed. Retrying...", idx)
			select {
			case <-this.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-this.notifyConfirm:
			if confirm.Ack {
				this.Debugf("队列%d: Push confirmed!", idx)
				break LOOP2
			}
		case <-time.After(resendDelay):
		}
		this.Debugf("队列%d: Push didn't confirm. Retrying...", idx)
	}
	return nil
}

func (this *Session) UnsafePush(data []byte, route string) error {
	if !this.isReady {
		return errNotConnected
	}
	return this.channel.Publish(
		this.exchangename, // Exchange
		route,             // Routing key
		false,             // Mandatory
		false,             // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		},
	)
}

func (this *Session) StreamAll() ([]<-chan amqp.Delivery, error) {
	if !this.isReady {
		return nil, errNotConnected
	}
	if this.ReChanSignal == nil {
		this.ReChanSignal = make(chan bool)
	}
	ret := make([]<-chan amqp.Delivery, 0)
	for _, queuename := range this.queuenames {
		csr, err := this.channel.Consume(
			queuename, "", false, false, false, false, nil,
		)
		if err != nil {
			return nil, err
		}
		ret = append(ret, csr)
	}
	return ret, nil
}

func (this *Session) StreamByIdx(i int) (ret <-chan amqp.Delivery, err error) {
	if !this.isReady {
		return nil, errNotConnected
	}
	if this.ReChanSignal == nil {
		this.ReChanSignal = make(chan bool)
	}
	if i >= len(this.queuenames) {
		return nil, errOutOfIdx
	}
	queuename := this.queuenames[i]
	ret, err = this.channel.Consume(
		queuename, "", false, false, false, false, nil,
	)
	return
}
