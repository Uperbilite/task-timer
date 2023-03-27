package scheduler

import (
	"context"
	"sync"

	"github.com/uperbilite/task-timer/common/conf"
	service "github.com/uperbilite/task-timer/service/scheduler"
)

// 读取配置启动多个协程进行
type WorkerApp struct {
	sync.Once
	service workerService
	ctx     context.Context
	stop    func()
}

func NewWorkerApp(service *service.Worker) *WorkerApp {
	w := WorkerApp{
		service: service,
	}

	w.ctx, w.stop = context.WithCancel(context.Background())
	return &w
}

func (w *WorkerApp) Start() {
	w.Do(w.start)
}

func (w *WorkerApp) start() {
	go func() {
		w.service.Start(w.ctx)
		// TODO: err handle
	}()
}

func (w *WorkerApp) Stop() {
	w.stop()
}

type workerService interface {
	Start(context.Context) error
}

type confProvider interface {
	Get() *conf.SchedulerAppConf
}
