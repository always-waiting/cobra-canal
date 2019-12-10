package event

import (
	"fmt"
	"github.com/MakeNowJust/heredoc"
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/schema"
)

const (
	UNKNOW_ACTION            = "未考虑的canal.RowsEvent动作(%s)"
	IDX_OUT_RANGE            = "下标越界(%d)"
	TABLE_NOT_DEFINE         = "表信息未定义"
	DATA_ROWNUM_ERR          = "原始数据条数不对"
	ONROW_ACTION_ERR         = "修改动作未定义"
	ONDDL_ERR1               = "操作动作为空"
	TYPE_ERR                 = "同步类型未定义"
	SYNC_TYPE_ONROW          = "OnRow"
	SYNC_TYPE_ONROTATE       = "OnRotate"
	SYNC_TYPE_ONTABLECHANGED = "OnTableChanged"
	SYNC_TYPE_ONDDL          = "OnDDL"
	SYNC_TYPE_ONXID          = "OnXID"
	SYNC_TYPE_ONGTID         = "OnGTID"
	SYNC_TYPE_ONPOSSYNCED    = "OnPosSynced"
	BASE_ERR1                = "未考虑的数据动作%s"
)

type Event struct {
	Table   *schema.Table   `description:"表对象,可以从中获得数据各类属性"`
	Type    string          `description:"同步类型,例如OnRow,OnDDL"`
	Action  string          `description:"具体动作,例如OnRow会有update,delete,insert"`
	RawData [][]interface{} `description:"原始数据"`
	Err     error           `description:"事件解析时的错误"`
	DDLSql  string          `description:"DDL语句"`
}

func (e *Event) Clone() (ret Event) {
	ret = Event{}
	table := *(e.Table)
	ret.Table = &table
	ret.Type = e.Type
	ret.Action = e.Action
	ret.RawData = e.RawData
	ret.Err = e.Err
	ret.DDLSql = e.DDLSql
	return
}

func (e *Event) GetNewData() (ret map[string]interface{}, err error) {
	switch e.Action {
	case "insert", "delete":
		ret, err = e.GetRowData(0)
	case "update":
		ret, err = e.GetRowData(1)
	default:
		err = errors.Errorf(BASE_ERR1, e.Action)
	}
	return
}

func (e *Event) GetColumnValue(row int, column string) (interface{}, error) {
	if row >= len(e.RawData) {
		return nil, errors.Errorf(IDX_OUT_RANGE, row)
	}
	rowData := e.RawData[row]
	return e.Table.GetColumnValue(column, rowData)
}

func (e *Event) GetRowData(row int) (ret map[string]interface{}, err error) {
	if row >= len(e.RawData) {
		return nil, errors.Errorf(IDX_OUT_RANGE, row)
	}
	ret = make(map[string]interface{})
	data := e.RawData[row]
	for _, column := range e.Table.Columns {
		var value interface{}
		value, err = e.Table.GetColumnValue(column.Name, data)
		if err != nil {
			ret = nil
			return
		}
		ret[column.Name] = value
	}
	return
}

func (e *Event) GetInt(row int, column string) (ret int, err error) {
	var i interface{}
	var ok bool
	if i, err = e.GetColumnValue(row, column); err != nil {
		return
	}
	if ret, ok = i.(int); !ok {
		err = errors.Errorf("%T无法转换为int", i)
		return
	}
	return
}

func (e *Event) GetInt8(row int, column string) (ret int8, err error) {
	var i interface{}
	var ok bool
	if i, err = e.GetColumnValue(row, column); err != nil {
		return
	}
	if ret, ok = i.(int8); !ok {
		err = errors.Errorf("%T无法转换为int8", i)
		return
	}
	return
}

func (e *Event) GetInt64(row int, column string) (ret int64, err error) {
	var i interface{}
	var ok bool
	if i, err = e.GetColumnValue(row, column); err != nil {
		return
	}
	if ret, ok = i.(int64); !ok {
		fmt.Printf("%T\n", i)
		err = errors.Errorf("%T无法转换为int64", i)
		return
	}
	return
}

func (e *Event) GetInt32(row int, column string) (ret int32, err error) {
	var i interface{}
	var ok bool
	if i, err = e.GetColumnValue(row, column); err != nil {
		return
	}
	if ret, ok = i.(int32); !ok {
		err = errors.Errorf("%T无法转换为int32", i)
		return
	}
	return
}

func (e *Event) GetString(row int, column string) (ret string, err error) {
	var i interface{}
	var ok bool
	if i, err = e.GetColumnValue(row, column); err != nil {
		return
	}
	if ret, ok = i.(string); !ok {
		err = errors.New("无法转换为string")
		return
	}
	return
}

func (e *Event) String() string {
	docTmp := heredoc.Doc(`[库名]: %s<br>[表名]: %s<br>[同步类型]: %s<br>`)
	switch e.Type {
	case SYNC_TYPE_ONROW:
		docTmp = heredoc.Doc(docTmp + `[动作]: %s<br>[数据]: %v<br>`)
		return fmt.Sprintf(docTmp, e.Table.Schema, e.Table.Name, e.Type,
			e.Action, e.RawData,
		)
	case SYNC_TYPE_ONDDL:
		docTmp = heredoc.Doc(docTmp + `[DDL]: %s<br>`)
		return fmt.Sprintf(docTmp, e.Table.Schema, e.Table.Name, e.Type,
			e.DDLSql,
		)
	default:
		return fmt.Sprintf(docTmp, e.Table.Schema, e.Table.Name, e.Type)
	}
}

func (e *Event) IsLegal() bool {
	e.Err = nil
	switch e.Type {
	case SYNC_TYPE_ONROW:
		if e.Table == nil {
			e.Err = errors.New(TABLE_NOT_DEFINE)
			return false
		}
		switch e.Action {
		case "insert", "delete":
			if len(e.RawData) != 1 {
				e.Err = errors.New(DATA_ROWNUM_ERR)
				return false
			} else {
				return true
			}
		case "update":
			if len(e.RawData) != 2 {
				e.Err = errors.New(DATA_ROWNUM_ERR)
				return false
			} else {
				return true
			}
		default:
			e.Err = errors.New(ONROW_ACTION_ERR)
			return false
		}
	case SYNC_TYPE_ONDDL:
		if e.DDLSql == "" {
			e.Err = errors.New(ONDDL_ERR1)
			return false
		}
		if e.Table == nil {
			e.Err = errors.New(TABLE_NOT_DEFINE)
			return false
		}
	default:
		e.Err = errors.New(TYPE_ERR)
		return false
	}
	return false
}
