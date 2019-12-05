package event

import (
	"regexp"
	"strings"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
)

const (
	EVENT_ERR1 = "解析DDL语句失败(%s)"
)

func ParseOnRow(rowevent *canal.RowsEvent) (events []Event, err error) {
	var step int
	switch rowevent.Action {
	case "update":
		step = 2
	case "insert", "delete":
		step = 1
	default:
		err = errors.Errorf(UNKNOW_ACTION, rowevent.Action)
		return
	}
	events = make([]Event, 0)
	for i := 0; i < len(rowevent.Rows); i = i + step {
		event := Event{Table: rowevent.Table, Type: SYNC_TYPE_ONROW, Action: rowevent.Action}
		event.RawData = make([][]interface{}, 0)
		for j := 0; j < step; j++ {
			data := rowevent.Rows[i+j]
			event.RawData = append(event.RawData, data)
		}
		event.IsLegal()
		events = append(events, event)
	}
	return
}

func ParseOnDDL(queryEvent *replication.QueryEvent) (e Event, err error) {
	e = Event{Type: SYNC_TYPE_ONDDL, DDLSql: string(queryEvent.Query)}
	e.Table = &schema.Table{
		Schema: string(queryEvent.Schema),
	}
	e.Table.Name, err = getTableNameByQuery(string(queryEvent.Query))
	e.IsLegal()
	return
}

func getTableNameByQuery(query string) (ret string, err error) {
	var flag bool
	if flag, err = regexp.MatchString("^alter|ALTER", query); err != nil {
		return
	}
	if !flag {
		return
	}
	var reg *regexp.Regexp
	if reg, err = regexp.Compile(`(table|TABLE) ([^\s]+)`); err != nil {
		return
	}
	matches := reg.FindSubmatch([]byte(query))
	if len(matches) == 0 {
		err = errors.Errorf(EVENT_ERR1, query)
		return
	}
	ret = strings.Trim(string(matches[2]), "`")
	return
}
