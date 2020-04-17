package config

import (
	"fmt"
	"github.com/siddontang/go-log/log"
	"os"
)

var DefaultLogCfg = &LogConfig{Level: "debug"}

func MakeFakeLogger() *log.Logger {
	h, _ := log.NewStreamHandler(os.Stdout)
	logger := log.NewDefault(h)
	return logger
}

type LogConfig struct {
	Level    string `toml:"level"`
	Type     string `toml:"type"`
	Filename string `toml:"filename" description:"日志文件地址，如果文件过大，需要清理，需要先mv(rm会产生错误)"`
	Dirname  string `toml:"dirname" descriotion:"日志文件目录"`
}

func (l *LogConfig) SetFilename(name string) {
	dirname := "."
	if l.Dirname != "" {
		dirname = l.Dirname
	}
	l.Filename = fmt.Sprintf("%s/%s", dirname, name)
}

func (l LogConfig) GetLogger() (logger *log.Logger, err error) {
	switch l.Type {
	case "file":
		var h *log.RotatingFileHandler
		h, err = log.NewRotatingFileHandler(l.Filename, 200*1024*1024, 5)
		if err != nil {
			return
		}
		logger = log.NewDefault(h)
	default:
		h, _ := log.NewStreamHandler(os.Stdout)
		logger = log.NewDefault(h)
	}
	if l.Level != "" {
		logger.SetLevelByName(l.Level)
	}
	return
}
