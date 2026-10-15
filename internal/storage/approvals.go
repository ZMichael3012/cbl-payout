package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Approval описывает строку журнала голосов операторов payout_approvals.
type Approval struct {
	// ID задает порядковый номер записи журнала.
	ID int64 `gorm:"column:id;primaryKey"`
	// PayoutID задает выплату голосования.
	PayoutID string `gorm:"column:payout_id"`
	// OperatorID задает оператора, оставившего голос.
	OperatorID string `gorm:"column:operator_id"`
	// Approve задает решение оператора (false - отклонение).
	Approve bool `gorm:"column:approve"`
	// Reason задает причину решения (для отклонения обязательна по смыслу).
	Reason string `gorm:"column:reason"`
	// Required задает требуемое число операторов на момент голоса (аудит политики).
	Required int `gorm:"column:required"`
	// CreatedAt задает время голоса.
	CreatedAt time.Time `gorm:"column:created_at"`
}

// TableName задает имя таблицы payout_approvals.
func (Approval) TableName() string { return "payout_approvals" }

// Transaction выполняет функцию в транзакции репозитория: голос оператора
// записывается атомарно с возможным переходом состояния (10.5). Ошибки
// возвращаются без обертки: вызывающий различает бизнес-ошибки через errors.Is.
func (r *Repository) Transaction(ctx context.Context, fn func(tx *gorm.DB) error) error {
	if fn == nil {
		return errors.New("storage: nil transaction function")
	}
	return r.db.WithContext(ctx).Transaction(fn)
}

// PayoutForUpdate возвращает выплату под row-локом в переданной транзакции:
// конкурентные голоса операторов сериализуются и не расходятся по счетчику.
func (r *Repository) PayoutForUpdate(ctx context.Context, tx *gorm.DB, id string) (Payout, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var payout Payout
	err := db.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", id).First(&payout).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Payout{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Payout{}, fmt.Errorf("storage: lock payout %s: %w", id, err)
	}
	return payout, nil
}

// RecordApproval добавляет голос оператора; повторный голос того же оператора
// идемпотентен (unique (payout_id, operator_id)) и возвращает false.
func (r *Repository) RecordApproval(ctx context.Context, tx *gorm.DB, approval Approval) (bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	approval.CreatedAt = time.Now().UTC()
	res := db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "payout_id"}, {Name: "operator_id"}},
			DoNothing: true,
		}).
		Create(&approval)
	if res.Error != nil {
		return false, fmt.Errorf("storage: record approval: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
}

// ApprovalByOperator возвращает голос оператора по выплате (если есть):
// сервис апрува сверяет решение повтора с зафиксированным (append-only).
func (r *Repository) ApprovalByOperator(ctx context.Context, tx *gorm.DB, payoutID, operatorID string) (Approval, bool, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var approval Approval
	err := db.WithContext(ctx).
		Where("payout_id = ? AND operator_id = ?", payoutID, operatorID).
		First(&approval).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Approval{}, false, nil
	}
	if err != nil {
		return Approval{}, false, fmt.Errorf("storage: approval by operator: %w", err)
	}
	return approval, true, nil
}

// ApprovalCount возвращает число голосов по выплате.
func (r *Repository) ApprovalCount(ctx context.Context, tx *gorm.DB, payoutID string) (int, error) {
	db := tx
	if db == nil {
		db = r.db
	}
	var count int64
	err := db.WithContext(ctx).Model(&Approval{}).
		Where("payout_id = ?", payoutID).
		Count(&count).Error
	if err != nil {
		return 0, fmt.Errorf("storage: approval count: %w", err)
	}
	return int(count), nil
}
