package config

type ManageConfig struct {
	Name           string             `toml:"name"`
	Desc           string             `toml:"desc"`
	BufNum         int                `toml:"buffer_number"`
	TableFilterCfg *TableFilterConfig `toml:"tablefilter" json:",omitempty"`
	Workers        []WorkerConfig     `toml:"workers" json:",omitempty"`
}

func (this *ManageConfig) HasTableFilter() bool {
	return this.TableFilterCfg != nil
}

func (this *ManageConfig) BufferNum() int {
	if this.BufNum == 0 {
		this.BufNum = DEFAULT_BUFFER
	}
	return this.BufNum
}
