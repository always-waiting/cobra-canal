package cobra

import (
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/always-waiting/cobra-canal/event"
	"github.com/siddontang/go-log/log"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

const (
	HEADER = "##############%s##############"
	DONE   = "##############Done##############"
)

func CreateHandlerV2(rulesCfg []config.RuleConfigV2) (h *HandlerV2, err error) {
	h = new(HandlerV2)
	return
}

type HandlerV2 struct {
	Log   *log.Logger
	errHr *cobraErrors.ErrHandlerV2
}

func (this *HandlerV2) OnRow(e *canal.RowsEvent) error {
	return nil
}

func (this *HandlerV2) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (this *HandlerV2) OnRotate(roateEvent *replication.RotateEvent) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONROTATE)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnTableChanged(schema string, table string) error {
	return nil
}
func (this *HandlerV2) OnXID(nextPos mysql.Position) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONXID)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnGTID(gtid mysql.GTIDSet) error {
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONGTID)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (this *HandlerV2) OnPosSynced(pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	if force {
		return nil
	}
	this.Log.Infof(HEADER, event.SYNC_TYPE_ONPOSSYNCED)
	defer func() { this.Log.Info(DONE) }()
	return nil
}

func (h *HandlerV2) String() string {
	return "HandlerV2"
}
