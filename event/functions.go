package event

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/replication"
	"io"
	"regexp"
	"strings"
)

var (
	ErrActionNotDefine = errors.New("mysql动作为定义")
	ErrInputEmpty      = errors.New("输入为空")
)

const (
	EVENT_ERR1 = "解析DDL语句失败(%s)"
)

func ParseOnRowV2(rowevent *canal.RowsEvent) (events []EventV2, err error) {
	var step int
	switch rowevent.Action {
	case "update":
		step = 2
	case "insert", "delete":
		step = 1
	default:
		err = ErrActionNotDefine
		return
	}
	events = make([]EventV2, 0)
	table := &Table{
		Schema: rowevent.Table.Schema,
		Name:   rowevent.Table.Name,
	}
	table.FillColumn(rowevent.Table)
	for i := 0; i < len(rowevent.Rows); i = i + step {
		event := EventV2{
			Type:   SYNC_TYPE_ONROW,
			Action: rowevent.Action,
			Table:  table,
		}
		event.RawData = make([][]interface{}, 0)
		for j := 0; j < step; j++ {
			data := rowevent.Rows[i+j]
			event.RawData = append(event.RawData, data)
		}
		if !event.IsLegal() {
			return nil, event.Err
		}
		events = append(events, event)
	}
	return
}

func ParseOnDDLV2(queryEvent *replication.QueryEvent) (e EventV2, err error) {
	e = EventV2{
		Type:   SYNC_TYPE_ONDDL,
		DDLSql: string(queryEvent.Query),
	}
	e.Table = &Table{
		Schema: string(queryEvent.Schema),
	}
	e.Table.Name, err = getTableNameByQuery(string(queryEvent.Query))
	if !e.IsLegal() {
		err = e.Err
	}
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
		err = errors.New(fmt.Sprintf(EVENT_ERR1, query))
		return
	}
	ret = strings.Trim(string(matches[2]), "`")
	return
}

func ToJSON(in interface{}) ([]byte, error) {
	return json.Marshal(in)
}

func Compress(in interface{}) ([]byte, error) {
	info, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	zw, err := flate.NewWriter(&buf, flate.BestCompression)
	if err != nil {
		return nil, err
	}
	defer zw.Close()
	if _, err = zw.Write(info); err != nil {
		return nil, err
	}
	zw.Flush()
	return buf.Bytes(), err
}

func Decompress(data []byte) ([]EventV2, error) {
	inbuffer := bytes.NewBuffer(data)
	flateReader := flate.NewReader(inbuffer)
	defer flateReader.Close()
	out := bytes.NewBuffer(nil)
	io.Copy(out, flateReader)
	return FromJSON(out.Bytes())
}

func FromJSON(data []byte) ([]EventV2, error) {
	if len(data) == 0 {
		return nil, ErrInputEmpty
	}
	if data[0] == 123 {
		obj := EventV2{}
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return []EventV2{obj}, nil
	} else {
		obj := make([]EventV2, 0)
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, err
		}
		return obj, nil
	}
}
