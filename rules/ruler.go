package rules

import (
	"net/http"

	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/siddontang/go-log/log"
)

type httpAction interface {
	ServeHTTPStop(http.ResponseWriter, *http.Response)
}

type Ruler interface {
	httpAction
	Start()
	Close() error
	HandleEvent(event.Event) error
	GetName() string
	SetLogger(*log.Logger)
	LoadConfig(config.RuleConfig) error
	SetNumber(int)
	GetNumber() int
	SetAggregator(config.Aggregatable)
	Reset() error
}
