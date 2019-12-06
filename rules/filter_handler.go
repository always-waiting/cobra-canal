package rules

import (
	"github.com/always-waiting/cobra-canal/config"
	"github.com/always-waiting/cobra-canal/event"
)

type FilterHandler []func(*event.Event) (bool, error)

func (this *FilterHandler) LoadTableFilter(cfg config.TableFilterable) {
	filter := func(e *event.Event) (bool, error) {
		flag := cfg.IsTablePass(e.Table.Schema, e.Table.Name)
		return flag, nil
	}
	this.AddFilterFunc(filter)
}

func (this *FilterHandler) LoadReplySyncFilter(list []string) {
	filter := func(e *event.Event) (bool, error) {
		if len(list) == 0 {
			return false, nil
		}
		for _, value := range list {
			if value == e.Type {
				return true, nil
			}
		}
		return false, nil
	}
	this.AddFilterFunc(filter)
}

func (this *FilterHandler) AddFilterFunc(f func(*event.Event) (bool, error)) {
	*this = append(*this, f)
}

func (this *FilterHandler) Filter(e *event.Event) (bool, error) {
	if len(*this) == 0 { // 为保证安全，没有过滤函数，默认不过
		return false, nil
	}
	for _, filterFunc := range *this {
		if flag, err := filterFunc(e); err != nil || !flag {
			return false, err
		}
	}
	return true, nil //所有过滤器全部通过，因此返回true
}
