package timer

import (
	"context"
	"github.com/uperbilite/task-timer/dao/task"

	"github.com/uperbilite/task-timer/common/model/po"
	"github.com/uperbilite/task-timer/pkg/mysql"

	"gorm.io/gorm"
)

type TimerDAO struct {
	client *mysql.Client
}

func NewTimerDAO(client *mysql.Client) *TimerDAO {
	return &TimerDAO{
		client: client,
	}
}

func (t *TimerDAO) CreateTimer(ctx context.Context, timer *po.Timer) (uint, error) {
	return timer.ID, t.client.DB.WithContext(ctx).Create(timer).Error
}

func (t *TimerDAO) DeleteTimer(ctx context.Context, id uint) error {
	return t.client.DB.WithContext(ctx).Delete(&po.Timer{Model: gorm.Model{ID: id}}).Error
}

func (t *TimerDAO) UpdateTimer(ctx context.Context, timer *po.Timer) error {
	return t.client.DB.WithContext(ctx).Updates(timer).Error
}

func (t *TimerDAO) GetTimer(ctx context.Context, opts ...Option) (*po.Timer, error) {
	db := t.client.DB.WithContext(ctx)
	for _, opt := range opts {
		db = opt(db)
	}
	var timer po.Timer
	return &timer, db.First(&timer).Error
}

func (t *TimerDAO) GetTimers(ctx context.Context, opts ...Option) ([]*po.Timer, error) {
	db := t.client.DB.WithContext(ctx).Model(&po.Timer{})
	for _, opt := range opts {
		db = opt(db)
	}
	var timers []*po.Timer
	return timers, db.Scan(&timers).Error
}

func (t *TimerDAO) CountTimers(ctx context.Context, opts ...Option) (int64, error) {
	db := t.client.DB.WithContext(ctx).Model(&po.Timer{})
	for _, opt := range opts {
		db = opt(db)
	}
	var cnt int64
	return cnt, db.Debug().Count(&cnt).Error
}

func (t *TimerDAO) DoWithLock(ctx context.Context, id uint, do func(ctx context.Context, timerDAO *TimerDAO, taskDAO *task.TaskDAO, timer *po.Timer) error) error {
	return t.client.Transaction(func(tx *gorm.DB) error {
		defer func() {
			if err := recover(); err != nil {
				tx.Rollback()
			}
		}()

		var timer po.Timer
		// 对当前的timer上锁，保证enable与unable的原子性
		if err := tx.Set("gorm:query_option", "FOR UPDATE").WithContext(ctx).First(&timer, id).Error; err != nil {
			return err
		}

		return do(ctx, NewTimerDAO(mysql.NewClient(tx)), task.NewTaskDAO(mysql.NewClient(tx)), &timer)
	})
}
