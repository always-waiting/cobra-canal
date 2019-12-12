package systemctl

import (
	"github.com/kardianos/service"
)

type Program struct{}

func (p *Program) Start(s service.Service) error {
	return nil
}

func (p *Program) Stop(s service.Service) error {
	return nil
}
