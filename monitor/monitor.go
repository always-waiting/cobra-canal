package monitor

import (
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

var (
	ErrRuleEmpty = errors.New("规则配置为空")
)

func MakeMonitor() (c *Monitor, err error) {
	c = &Monitor{}
	c.cfg = config.ConfigV2()
	if err = c.SetLog(); err != nil {
		return
	}
	if err = c.SetCobraDB(); err != nil {
		return
	}
	if err = c.SetCanal(); err != nil {
		return
	}
	if err = c.SetMonitorPos(); err != nil {
		return
	}
	if err = c.SetErrHr(); err != nil {
		return
	}
	if err = c.SetHandler(); err != nil {
		return
	}
	if err = c.SetHttp(); err != nil {
		return
	}
	return
}

type Monitor struct {
	Canal           *canal.Canal              `description:"从库对象"`
	Http            *MonitorHttpV2            `description:"交互http服务"`
	Handler         *HandlerV2                `description:"处理事件的对象"`
	ErrHr           *cobraErrors.ErrHandlerV2 `description:"错误处理对象"`
	Log             *log.Logger               `description:"日志"`
	CobraDb         *gorm.DB                  `description:"眼镜蛇数据库"`
	startMonitorPos *cmysql.Position
	cfg             *config.ConfigureV2
}

func (this *Monitor) Cfg() *config.ConfigureV2 {
	return this.cfg
}

func (this *Monitor) RulesCfg() []config.RuleConfigV2 {
	return this.cfg.RulesCfg
}

func (this *Monitor) SetLog() (err error) {
	this.cfg.CobraCfg.LogCfg.SetFilename("cobra.log")
	this.Log, err = this.cfg.CobraCfg.LogCfg.GetLogger()
	return
}

func (this *Monitor) SetHandler() (err error) {
	defer func() {
		if err == nil && this.Log != nil {
			this.Log.Debug("SetHandler: 成功")
		}
	}()
	defer this.Recover(&err)
	h, err := CreateHandlerV2(this)
	if err != nil {
		return
	}
	this.Handler = h
	this.Canal.SetEventHandler(this.Handler)
	return
}

func (this *Monitor) SetHttp() (err error) {
	defer func() {
		if err == nil && this.Log != nil {
			this.Log.Debug("SetHttp: 成功")
		}
	}()
	defer this.Recover(&err)
	this.Log.Debug("SetHttp: 初始化Master Http...")
	return
}

func (this *Monitor) SetErrHr() (err error) {
	cfg := this.cfg.CobraCfg
	defer func() {
		if err == nil && this.Log != nil {
			this.Log.Debug("SetErrHr: 成功")
		}
	}()
	defer this.Recover(&err)
	this.Log.Debug("SetErrHr: 初始化错误处理器...")
	eHr := cfg.ErrCfg.MakeHandler()
	this.ErrHr = &eHr
	return
}

func (this *Monitor) SetMonitorPos() (err error) {
	defer func() {
		this.Recover(&err)
		if err == nil && this.Log != nil {
			this.Log.Debug("SetMonitorPos: 成功")
		}
	}()
	this.Log.Debug("SetMonitorPos: 获取监控点...")
	if this.cfg.CobraCfg.Rebase {
		this.startMonitorPos, err = this.getCurrentPosition()
	} else {
		pos := PosInfo{}
		err = this.CobraDb.Where("service_id = ?", this.cfg.CobraCfg.Config.ServerID).
			Take(&pos).Error
		if err != nil {
			return
		}
		if pos.ServiceId == 0 {
			this.startMonitorPos, err = this.getCurrentPosition()
		} else {
			this.startMonitorPos = &cmysql.Position{
				Name: pos.File,
				Pos:  pos.Pos,
			}
		}
	}
	return
}

func (this *Monitor) getCurrentPosition() (pos *cmysql.Position, err error) {
	row, err := this.Canal.Execute("show master status")
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

func (this *Monitor) Recover(err *error) {
	if e := recover(); e != nil {
		*err = errors.Errorf("Monitor未知错误:%v", e)
	}
}

func (this *Monitor) SetCanal() (err error) {
	defer func() {
		if err == nil && this.Log != nil {
			this.Log.Debug("CreateCanal: 成功")
		}
	}()
	defer this.Recover(&err)
	cfg := this.cfg.CobraCfg
	this.Log.Debug("CreateCanal: 初始化Canal...")
	this.Canal, err = canal.NewCanal(cfg.Config)
	return
}

func (this *Monitor) SetCobraDB() (err error) {
	cfg := this.cfg.CobraCfg
	defer func() {
		if err == nil && this.Log != nil {
			this.Log.Debug("LinkCobraDB: 成功")
		}
	}()
	defer this.Recover(&err)
	this.Log.Debug("LinkCobraDB: 链接监控信息库...")
	var gormAddr string
	if gormAddr, err = cfg.DbCfg.ToGormAddr(); err != nil {
		return
	}
	if this.CobraDb, err = gorm.Open("mysql", gormAddr); err != nil {
		return
	}
	return
}

func (this *Monitor) syncedPosition() (pos *cmysql.Position) {
	tmp := this.Canal.SyncedPosition()
	pos = &tmp
	return
}

func (this *Monitor) SavePosition() (pos *cmysql.Position, err error) {
	pos = this.syncedPosition()
	defer func() {
		if err == nil {
			this.startMonitorPos = pos
		}
	}()
	defer this.Recover(&err)
	posInfo := PosInfo{}
	err = this.CobraDb.Where("service_id = ?", this.cfg.CobraCfg.Config.ServerID).
		Take(&posInfo).Error
	if err != nil && !gorm.IsRecordNotFoundError(err) {
		return
	} else {
		err = nil
	}
	if posInfo.ServiceId == 0 {
		ip, err := this.getRunningIp()
		if err != nil {
			return pos, err
		}
		desc := fmt.Sprintf("%s:%s", ip, this.cfg)
		posInfo.ServiceId, posInfo.File, posInfo.Pos, posInfo.Desc =
			this.cfg.CobraCfg.Config.ServerID, pos.Name, pos.Pos, desc
		err = this.CobraDb.Create(posInfo).Error
	} else {
		err = this.CobraDb.Model(&posInfo).Where("service_id = ?", posInfo.ServiceId).Update(map[string]interface{}{
			"binlog_file":     pos.Name,
			"binlog_position": pos.Pos,
		}).Error
	}
	return
}

func (this *Monitor) getRunningIp() (ip string, err error) {
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

func (this *Monitor) Run() {
	go this.ErrHr.Send()
	defer this.ErrHr.Close()
	err := this.Canal.RunFrom(*this.startMonitorPos)
	if err != nil {
		this.ErrHr.Push(err)
	}
	if pos, err := this.SavePosition(); err != nil {
		this.Log.Errorf("保存监控点失败%s", err)
		this.ErrHr.Push(err)
	} else {
		this.Log.Debugf("保存监控点:%#v", pos)
	}
	return
}

func (this *Monitor) Close() {
	this.Canal.Close()
}
