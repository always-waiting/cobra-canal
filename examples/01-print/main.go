package main

/*
示例监控库中id为1的数据变化，并把变化打印到文件中。
具体打印地址，参见配置文件
*/

import (
	"errors"
	"fmt"
	"os"

	"github.com/always-waiting/cobra-canal/cmd"
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/consumer"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/always-waiting/cobra-canal/rules"
)

type rule struct {
	rules.BasicRuler
}

func makeRule(cfg config.RuleConfig) (ret rules.Ruler, err error) {
	r := new(rule)
	r.SetName(cfg.Name)
	r.SetDesc("只保留id为1的数据")
	r.AddFilterFunc(rule1)
	r.AddTransferFunc("print", makePrintData)
	ret = r
	return
}

func makePrintData(data []event.Event) (sendData interface{}, err error) {
	byteStr := []byte(fmt.Sprintf("%v\n", data))
	sendData = byteStr
	return
}

func rule1(e *event.Event) (bool, error) {
	id, err := e.GetInt32(0, "id")
	if err != nil {
		return false, err
	}
	if id != 1 {
		return false, nil
	}
	return true, nil
}

type printConsumer struct {
	consumer.BaseConsumer
	file   string
	fileIo *os.File
}

func (c *printConsumer) Open() (err error) {
	c.fileIo, err = os.OpenFile(c.file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	return
}

func (c *printConsumer) Close() (err error) {
	return c.fileIo.Close()
}

func (c *printConsumer) Solve(i interface{}) (err error) {
	content, ok := i.([]byte)
	if !ok {
		err = errors.New("转换为[]byte错误")
		return
	}
	_, err = c.fileIo.Write(content)
	return
}

func makeconsumer(cfg *config.ConsumerConfig) (ret consumer.Consumer, err error) {
	c := new(printConsumer)
	c.file = cfg.App
	c.SetName(cfg.Type)
	ret = c
	return
}

func main() {
	rules.RegisterRuleMaker("test", makeRule)
	consumer.RegisterConsumerMaker("print", makeconsumer)
	cmd.Execute()
}
