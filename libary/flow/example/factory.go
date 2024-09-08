package example

import (
	"errors"

	"github.com/longpi1/gopkg/libary/flow"
)

var TaskMap = map[string]func(name string) flow.Task{
	"a": NewExample,
}

func Factory(name string) (task flow.Task, err error) {
	if factory, has := TaskMap[name]; has {
		return factory(name), nil
	}
	return nil, errors.New("not found")
}
