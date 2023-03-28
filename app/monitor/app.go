package monitor

import (
	"context"
	"sync"

	service "github.com/uperbilite/task-timer/service/monitor"
)

type MonitorApp struct {
	sync.Once
	ctx    context.Context
	stop   func()
	worker *service.Worker
}

func NewMonitorApp(worker *service.Worker) *MonitorApp {
	m := MonitorApp{
		worker: worker,
	}

	m.ctx, m.stop = context.WithCancel(context.Background())
	return &m
}

func (m *MonitorApp) Start() {
	m.Do(func() {
		go m.worker.Start(m.ctx)
	})
}

func (m *MonitorApp) Stop() {
	m.stop()
}
