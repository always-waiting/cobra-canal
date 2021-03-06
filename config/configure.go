package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"os"
)

const (
	LOGCONFIG_ERR1        = "log-file需要定义"
	DEFAULT_PORT          = 6543
	DEFAULT_BUFFER_NUMBER = 1000 * 100
)

func (c *configure) SetLog() (err error) {
	switch c.LogCfg.Type {
	case "file":
		err = c.SetLogFile()
	default:
		c.SetLevel()
		log.Info("默认输出到屏幕")
	}
	return
}

func (c *configure) String() string {
	return c.path
}

func (c *configure) GetBufferNum() int {
	if c.BufferNum == 0 {
		log.Infof("handle使用默认缓存长度: %d", DEFAULT_BUFFER_NUMBER)
		return DEFAULT_BUFFER_NUMBER
	}
	return c.BufferNum
}

func (c *configure) GetPort() int {
	if c.Port == 0 {
		log.Infof("使用默认端口: %d", DEFAULT_PORT)
		return DEFAULT_PORT
	}
	return c.Port
}

func (c *configure) SetLogFile() (err error) {
	if c.LogCfg.Filename == "" {
		err = errors.New(LOGCONFIG_ERR1)
		return
	}
	var file *os.File
	if file, err = os.Create(c.LogCfg.Filename); err != nil {
		return
	}
	var h *log.StreamHandler
	if h, err = log.NewStreamHandler(file); err != nil {
		return
	}
	l := log.NewDefault(h)
	log.SetDefaultLogger(l)
	c.SetLevel()
	go c.DaemonLogFile(file)
	return
}

func (c *configure) SetLevel() {
	if c.LogCfg.Level != "" {
		log.SetLevelByName(c.LogCfg.Level)
	}
}

func (c *configure) DaemonLogFile(file *os.File) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()
	done := make(chan bool)
	go func() {
	loop:
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Rename == fsnotify.Rename ||
					event.Op&fsnotify.Remove == fsnotify.Remove {
					file.Close()
					break loop
				}
			case _ = <-watcher.Errors:
				file.Close()
				break loop
			}
		}
		done <- true
	}()
	err = watcher.Add(c.LogCfg.Filename)
	if err != nil {
		panic(err)
	}
	<-done
	c.SetLogFile()
}
