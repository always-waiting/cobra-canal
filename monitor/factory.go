package monitor

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/rules/consumer"
	"github.com/always-waiting/cobra-canal/rules/filter"
)

type Factory struct {
	cfg      config.RuleConfigV2
	Filter   *filter.Manager
	Consumer *consumer.Manager
}

func CreateFactory(rule config.RuleConfigV2) (ret *Factory, err error) {
	ret = &Factory{cfg: rule}
	fmanager, err := filter.CreateManagerWithNext(rule)
	if err != nil {
		return nil, err
	}
	cmanager, err := consumer.CreateManagerWithNext(rule)
	cmanager.SetSession(fmanager.Next.GetSession())
	ret.Filter = fmanager
	ret.Consumer = cmanager
	return ret, nil
}

func (this *Factory) Id() int {
	return this.cfg.Id
}

func (this *Factory) Start() {
	go this.Filter.Start()
	go this.Consumer.Start()
}

func (this *Factory) Close() {
	this.Filter.Close()
	this.Consumer.Close()
}
