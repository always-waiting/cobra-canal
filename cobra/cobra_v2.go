package cobra

import (
	cobraErrors "github.com/always-waiting/cobra-canal/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
)

type CobraV2 struct {
	Canal   *canal.Canal            `description:"从库对象"`
	Rebase  bool                    `description:"是否重置监控点"`
	Handler *HandlerV2              `description:"处理事件的对象"`
	ErrHr   *cobraErrors.ErrHandler `description:"错误处理对象"`
	Log     *log.Logger             `description:"日志"`
}

func MakeCobraV2() (c *CobraV2, err error) {
}
