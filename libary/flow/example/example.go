package example

import (
	"context"

	"github.com/longpi1/gopkg/libary/flow"
)

type Example struct {
	Name string
}

func NewExample(name string) flow.Task {
	return &Example{Name: name}
}

func (e Example) NodeName() string {
	return e.Name
}

// Run 具体实现
func (e Example) Run(ctx context.Context, set flow.DataSet) error {
	return nil
}
