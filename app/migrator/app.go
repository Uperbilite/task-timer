package migrator

import (
	"context"
	"sync"

	service "github.com/uperbilite/task-timer/service/migrator"
)

// 定期从 timer 表中加载一系列 task 记录添加到 task 表中
type MigratorApp struct {
	sync.Once
	ctx    context.Context
	stop   func()
	worker *service.Worker
}

func NewMigratorApp(worker *service.Worker) *MigratorApp {
	m := MigratorApp{
		worker: worker,
	}

	m.ctx, m.stop = context.WithCancel(context.Background())
	return &m
}

func (m *MigratorApp) Start() {
	m.Do(func() {
		go func() {
			m.worker.Start(m.ctx)
		}()
	})
}

func (m *MigratorApp) Stop() {
	m.stop()
}
