package cobra

import (
	"database/sql"
	"fmt"
	"github.com/always-waiting/cobra-canal/config"
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	cmysql "github.com/siddontang/go-mysql/mysql"
	"net"
)

const (
	QUERY_POS_SQL  = "select service_id,binlog_file,binlog_position,gtid from t_positions where service_id=?"
	INSERT_POS_SQL = "insert into t_positions (service_id, binlog_file, binlog_position,`desc`) values (?,?,?,?)"
	UPDATE_POS_SQL = "update t_positions set binlog_file=?, binlog_position=?, `desc`=? where service_id=?"
)

type Cobra struct {
	Canal   *canal.Canal            `description:"从库对象"`
	Rebase  bool                    `description:"是否重置监控点"`
	Handler *Handler                `description:"处理对象"`
	CobraDb *gorm.DB                `description:"眼镜蛇数据库"`
	ErrHr   *cobraErrors.ErrHandler `description:"错误处理对象"`
	Log     *log.Logger             `description:"日志"`
}

func MakeCobra() (c *Cobra, err error) {
	c = new(Cobra)
	cfg := config.Config()
	c.Rebase = cfg.RebaseFlag
	c.Log, err = cfg.LogCfg.GetLogger()
	if err != nil {
		return
	}
	c.Log.Debug("初始化Handler...")
	if c.Handler, err = CreateHandler(cfg.RulesCfg, cfg.GetBufferNum()); err != nil {
		return
	}
	c.Handler.Log = c.Log
	if cfg.RebaseFlag {
		c.Log.Info("重新定位监控点")
	}
	c.Log.Debug("初始化Canal...")
	if c.Canal, err = canal.NewCanal(cfg.CanalCfg); err != nil {
		return
	}
	// 初始化CobraDb属性
	c.Log.Debug("初始化CobraDb...")
	var gormAddr string
	if gormAddr, err = cfg.MysqlCfg.ToGormAddr(); err != nil {
		return
	}
	var gormDb *gorm.DB
	if gormDb, err = gorm.Open("mysql", gormAddr); err != nil {
		c.Log.Errorf("错误gorm地址:%s", gormAddr)
		return
	}
	c.CobraDb = gormDb
	c.Canal.SetEventHandler(c.Handler)
	c.Log.Debug("初始化错误处理器")
	sender := cfg.ErrSenderCfg.Parse()
	c.ErrHr = cobraErrors.MakeErrHandler(sender, 10)
	c.Handler.errHr = c.ErrHr
	c.Log, err = cfg.LogCfg.GetLogger()
	return
}

func (c *Cobra) getMonitorStartPosition() (pos *cmysql.Position, err error) {
	cfg := config.Config()
	if !c.Rebase {
		row := c.CobraDb.Raw(QUERY_POS_SQL, cfg.CanalCfg.ServerID).Row()
		var file, gtid string
		var serverId, positionNum int
		if err = row.Scan(&serverId, &file, &positionNum, &gtid); err != nil {
			if err == sql.ErrNoRows {
				if pos, err = c.getCurrentPosition(); err != nil {
					return
				}
			} else {
				return
			}
		} else {
			pos = new(cmysql.Position)
			pos.Name = file
			pos.Pos = uint32(positionNum)
		}
	} else {
		if pos, err = c.getCurrentPosition(); err != nil {
			return
		}
	}
	return
}

func (c *Cobra) getCurrentPosition() (pos *cmysql.Position, err error) {
	row, err := c.Canal.Execute("show master status")
	if err != nil {
		panic(err)
	}
	file, err := row.GetStringByName(0, "File")
	if err != nil {
		panic(err)
	}
	idx, err := row.GetIntByName(0, "Position")
	if err != nil {
		panic(err)
	}
	pos = new(cmysql.Position)
	pos.Name = file
	pos.Pos = uint32(idx)
	return
}

func (c *Cobra) getRunningIp() (ip string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok &&
			!ipnet.IP.IsLoopback() &&
			ipnet.IP.To4() != nil {
			ip = ipnet.IP.String()
			return
		}
	}
	if len(addrs) == 0 {
		err = errors.New("没有发现ip信息!")
	}
	return
}

func (c *Cobra) SavePosition() (err error) {
	cfg := config.Config()
	pos := c.syncedPosition()
	row := c.CobraDb.Raw(QUERY_POS_SQL, cfg.CanalCfg.ServerID).Row()
	ip, err := c.getRunningIp()
	if err != nil {
		return
	}
	desc := fmt.Sprintf("%s:%s", ip, cfg)
	//log.Infof("运行的服务器信息为:%s", desc)

	var file, gtid string
	var serverId, positionNum int
	if err = row.Scan(&serverId, &file, &positionNum, &gtid); err != nil {
		if err == sql.ErrNoRows {
			// 插入
			err = c.CobraDb.Exec(INSERT_POS_SQL, cfg.CanalCfg.ServerID, pos.Name, pos.Pos, desc).Error
		} else {
			return
		}
	} else {
		// 更新
		err = c.CobraDb.Exec(UPDATE_POS_SQL, pos.Name, pos.Pos, desc, cfg.CanalCfg.ServerID).Error
	}
	return
}

func (c *Cobra) syncedPosition() (pos *cmysql.Position) {
	tmp := c.Canal.SyncedPosition()
	pos = &tmp
	return
}

func (c *Cobra) Run() error {
	c.Handler.Start()
	pos, err := c.getMonitorStartPosition()
	if err != nil {
		return err
	}
	c.Log.Debug("开启cobra&handler的错误处理器")
	go c.ErrHr.Send()
	c.Log.Debug("开启器监控")
	err = c.Canal.RunFrom(*pos)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cobra) Close() {
	c.Canal.Close()
	c.Log.Debug("关闭binlog接收器")
	c.Handler.Stop()
	c.Log.Debug("处理器关闭")
	err := c.SavePosition()
	if err != nil {
		c.Log.Errorf("保存监控点失败%s", err)
	} else {
		c.Log.Debug("保存监控点")
	}
	c.ErrHr.Close()
	c.Log.Debug("关闭cobra&handler的错误处理器")
}
