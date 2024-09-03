package db

import (
	"time"

	"gorm.io/gorm"
)

// Model 公共Model
type Model struct {
	ID        int64 `gorm:"primary_key" json:"id"`
	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
	DeletedAt int64 `json:"deleted_at"`
	IsDel     int64 `gorm:"softDelete:flag" json:"is_del"`
}

type ConditionsT map[string]any
type Predicates map[string][]any

func (m *Model) BeforeCreate(tx *gorm.DB) (err error) {
	nowTime := time.Now().Unix()

	tx.Statement.SetColumn("created_at", nowTime)
	tx.Statement.SetColumn("updated_at", nowTime)
	return
}

func (m *Model) BeforeUpdate(tx *gorm.DB) (err error) {
	if !tx.Statement.Changed("updated_at") {
		tx.Statement.SetColumn("updated_at", time.Now().Unix())
	}

	return
}
