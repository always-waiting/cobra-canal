package event

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
	"io"
	"reflect"
)

const (
	SYNC_TYPE_ONROW          = "OnRow"
	SYNC_TYPE_ONROTATE       = "OnRotate"
	SYNC_TYPE_ONTABLECHANGED = "OnTableChanged"
	SYNC_TYPE_ONDDL          = "OnDDL"
	SYNC_TYPE_ONXID          = "OnXID"
	SYNC_TYPE_ONGTID         = "OnGTID"
	SYNC_TYPE_ONPOSSYNCED    = "OnPosSynced"
)

var (
	ErrOutIdx         = errors.New("下标越界")
	ErrTableNotDefine = errors.New("表信息未定义")
	ErrRowNum         = errors.New("原始数据条数不对")
	ErrOnDDLEmpty     = errors.New("操作动作为空")
	ErrSyncNotDefine  = errors.New("同步类型未定义")
	ErrTypeErr        = errors.New("interface转换类型出错")
)

type Table struct {
	Schema  string
	Name    string
	Columns []string
}

func (this *Table) GetColumnValue(column string, data []interface{}) (interface{}, error) {
	index := this.FindColumn(column)
	if index == -1 {
		return nil, errors.Errorf("table %s has no column name %s", this, column)
	}
	return data[index], nil
}

func (this *Table) String() string {
	return fmt.Sprintf("%s.%s", this.Schema, this.Name)
}

func (this *Table) FillColumn(ta *schema.Table) {
	this.Columns = make([]string, 0)
	for _, col := range ta.Columns {
		this.Columns = append(this.Columns, col.Name)
	}
}

func (this *Table) FindColumn(name string) int {
	for i, col := range this.Columns {
		if col == name {
			return i
		}
	}
	return -1
}

type EventV2 struct {
	Table   *Table
	RawData [][]interface{}
	Type    string
	Action  string
	Err     error
	DDLSql  string
}

func (this EventV2) String() string {
	info, _ := this.ToJSON()
	return string(info)
}

func (this EventV2) ToJSON() ([]byte, error) {
	return json.Marshal(this)
}

func (this *EventV2) FromJSON(data []byte) error {
	return json.Unmarshal(data, this)
}

func (this *EventV2) Decompress(data []byte) error {
	inbuffer := bytes.NewBuffer(data)
	flateReader := flate.NewReader(inbuffer)
	defer flateReader.Close()
	out := bytes.NewBuffer(nil)
	io.Copy(out, flateReader)
	return this.FromJSON(out.Bytes())
}

func (this EventV2) Compress() ([]byte, error) {
	info, err := this.ToJSON()
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

func (this *EventV2) GetColumnValue(row int, column string) (interface{}, error) {
	if row >= len(this.RawData) {
		return nil, ErrOutIdx
	}
	rowData := this.RawData[row]
	return this.Table.GetColumnValue(column, rowData)
}

func (e *EventV2) IsLegal() bool {
	e.Err = nil
	switch e.Type {
	case SYNC_TYPE_ONROW:
		if e.Table == nil {
			e.Err = ErrTableNotDefine
			return false
		}
		switch e.Action {
		case "insert", "delete":
			if len(e.RawData) != 1 {
				e.Err = ErrRowNum
				return false
			} else {
				return true
			}
		case "update":
			if len(e.RawData) != 2 {
				e.Err = ErrRowNum
				return false
			} else {
				return true
			}
		default:
			e.Err = ErrActionNotDefine
			return false
		}
	case SYNC_TYPE_ONDDL:
		if e.DDLSql == "" {
			e.Err = ErrOnDDLEmpty
			return false
		}
		if e.Table == nil {
			e.Err = ErrTableNotDefine
			return false
		}
	default:
		e.Err = ErrSyncNotDefine
		return false
	}
	return true
}

func (this *EventV2) GetInt64(row int, column string) (ret int64, err error) {
	var i interface{}
	if i, err = this.GetColumnValue(row, column); err != nil {
		return
	}
	switch i.(type) {
	case int, int8, int32, int64, uint, uint8, uint32, uint64:
		ret = reflect.ValueOf(i).Int()
	default:
		err = ErrTypeErr
	}
	return
}

func (this *EventV2) GetFloat(row int, column string) (ret float64, err error) {
	var i interface{}
	if i, err = this.GetColumnValue(row, column); err != nil {
		return
	}
	switch i.(type) {
	case float64, float32:
		ret = reflect.ValueOf(i).Float()
	default:
		err = ErrTypeErr
	}
	return
}
