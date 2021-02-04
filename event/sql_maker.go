package event

import (
	"fmt"
	"github.com/juju/errors"
	"strings"
)

const (
	SQL_UPDATE_TEMPLATE = "UPDATE %s SET %s WHERE %s"
	SQL_INSERT_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s)"
	SQL_DELETE_TEMPLATE = "DELETE FROM %s WHERE %s"
	SQL_DDL_TEMPLATE    = "%s"
	MYSQL_ERR1          = "未考虑的同步类型(%s)"
	MYSQL_ERR2          = "数据更新发现新sql模板(%s)"
	MYSQL_ERR3          = "DDL语句为空"
)

type sqlMaker struct {
	syncType     string                 `description:"同步类型"`
	sqlBase      string                 `description:"sql的模板"`
	searchFields map[string]interface{} `description:"查询条件"`
	valueFields  map[string]interface{} `description:"处理的数据"`
	table        string                 `description:"操作的表"`
	ddlSql       string                 `description:"ddl语句"`
}

func CreateSqlMaker(e Event) (msql sqlMaker, err error) {
	msql = sqlMaker{}
	msql.table = e.Table.Name
	msql.syncType = e.Type
	if err = msql.setSQLBase(e.Action); err != nil {
		return
	}
	if err = msql.parseEvent(e); err != nil {
		return
	}
	return
}

func (m *sqlMaker) parseEvent(e Event) error {
	switch m.syncType {
	case SYNC_TYPE_ONROW:
		return m.parseOnRow(e)
	case SYNC_TYPE_ONDDL:
		return m.parseOnDDL(e)
	default:
		return errors.Errorf(MYSQL_ERR2, m.syncType)
	}
	return nil
}

func (m *sqlMaker) setSQLBase(action string) error {
	switch m.syncType {
	case SYNC_TYPE_ONROW:
		switch action {
		case "insert":
			m.sqlBase = SQL_INSERT_TEMPLATE
		case "update":
			m.sqlBase = SQL_UPDATE_TEMPLATE
		case "delete":
			m.sqlBase = SQL_DELETE_TEMPLATE
		}
	case SYNC_TYPE_ONDDL:
		m.sqlBase = SQL_DDL_TEMPLATE
	default:
		return errors.Errorf(MYSQL_ERR1, m.syncType)
	}
	return nil
}

func (m *sqlMaker) parseOnDDL(e Event) error {
	if e.DDLSql == "" {
		return errors.New(MYSQL_ERR3)
	}
	m.ddlSql = e.DDLSql
	return nil
}

func (m *sqlMaker) parseOnRow(e Event) error {
	if m.syncType != SYNC_TYPE_ONROW {
		return nil
	}
	switch e.Action {
	case "update":
		rawMap, err := e.GetRowData(1)
		if err != nil {
			return err
		}
		m.searchFields = make(map[string]interface{})
		for num, _ := range e.Table.PKColumns {
			pkColumn := e.Table.GetPKColumn(num)
			m.searchFields[pkColumn.Name] = rawMap[pkColumn.Name]
			delete(rawMap, pkColumn.Name)
		}
		if len(m.searchFields) == 0 { // 没有主键
			m.searchFields, err = e.GetRowData(0)
			if err != nil {
				return err
			}
		}
		m.valueFields = rawMap
	case "insert":
		rawMap, err := e.GetRowData(0)
		if err != nil {
			return err
		}
		m.valueFields = rawMap
	case "delete":
		rawMap, err := e.GetRowData(0)
		if err != nil {
			return err
		}
		m.searchFields = make(map[string]interface{})
		for num, _ := range e.Table.PKColumns {
			pkColumn := e.Table.GetPKColumn(num)
			m.searchFields[pkColumn.Name] = rawMap[pkColumn.Name]
		}
		if len(m.searchFields) == 0 {
			m.searchFields = rawMap
		}
	}
	return nil
}

func (m sqlMaker) String() string {
	if m.sqlBase == "" {
		return ""
	}
	switch m.syncType {
	case SYNC_TYPE_ONROW:
		return m.onRowString()
	case SYNC_TYPE_ONDDL:
		return m.onDDLString()
	}
	return ""
}

func (m sqlMaker) onDDLString() string {
	return fmt.Sprintf(m.sqlBase, m.ddlSql)
}

func (m sqlMaker) onRowString() string {
	if m.table == "" {
		return ""
	}
	if strings.HasPrefix(m.sqlBase, "INSERT") {
		cols := make([]string, 0)
		vals := make([]string, 0)
		for key, value := range m.valueFields {
			cols = append(cols, fmt.Sprintf("`%s`", key))
			if value == nil {
				vals = append(vals, "null")
			} else if valBytes, ok := value.([]byte); ok {
				vals = append(vals, fmt.Sprintf("'%v'", string(valBytes)))
			} else {
				vals = append(vals, fmt.Sprintf("'%v'", value))
			}
		}
		return fmt.Sprintf(m.sqlBase, m.table, strings.Join(cols, ","), strings.Join(vals, ","))

	} else if strings.HasPrefix(m.sqlBase, "UPDATE") {
		values := make([]string, 0)
		search := make([]string, 0)
		for key, value := range m.valueFields {
			if value == nil {
				values = append(values, fmt.Sprintf("`%s`='null'", key))
			} else if valBytes, ok := value.([]byte); ok {
				values = append(values, fmt.Sprintf("`%s`='%v'", key, string(valBytes)))
			} else {
				values = append(values, fmt.Sprintf("`%s`='%v'", key, value))
			}
		}
		for key, value := range m.searchFields {
			search = append(search, fmt.Sprintf("`%s`='%v'", key, value))
		}
		return fmt.Sprintf(m.sqlBase, m.table, strings.Join(values, ","), strings.Join(search, " and "))
	} else if strings.HasPrefix(m.sqlBase, "DELETE") {
		search := make([]string, 0)
		for key, value := range m.searchFields {
			search = append(search, fmt.Sprintf("`%s`='%v'", key, value))
		}
		return fmt.Sprintf(m.sqlBase, m.table, strings.Join(search, " and "))
	} else {
		return ""
	}
	return ""
}
