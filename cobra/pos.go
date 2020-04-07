package cobra

type PosInfo struct {
	ServiceId uint32 `gorm:"column:service_id"`
	File      string `gorm:"column:binlog_file"`
	Pos       uint32 `gorm:"column:binlog_position"`
	Desc      string `gorm:"column:desc"`
}

func (this PosInfo) TableName() string {
	return "t_positions"
}
