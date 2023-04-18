package trigger

import (
	"context"
	"fmt"
	"github.com/uperbilite/task-timer/common/conf"
	"github.com/uperbilite/task-timer/common/model/vo"
	"github.com/uperbilite/task-timer/common/utils"
	"github.com/uperbilite/task-timer/pkg/pool"
	"github.com/uperbilite/task-timer/service/executor"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Worker struct {
	task         taskService
	confProvider confProvider
	pool         pool.WorkerPool
	executor     *executor.Worker
}

func NewWorker(executor *executor.Worker, task *TaskService, confProvider *conf.TriggerAppConfProvider) *Worker {
	return &Worker{
		executor:     executor,
		task:         task,
		pool:         pool.NewGoWorkerPool(confProvider.Get().WorkersNum),
		confProvider: confProvider,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.executor.Start(ctx)
}

func (w *Worker) Work(ctx context.Context, minuteBucketKey string, ack func()) error {
	// 进行为时一分钟的 zrange 处理
	startTime, err := getStartMinute(minuteBucketKey)
	if err != nil {
		return err
	}
	endTime := startTime.Add(time.Minute)

	config := w.confProvider.Get()

	ticker := time.NewTicker(time.Duration(config.ZRangeGapSeconds) * time.Second)
	defer ticker.Stop()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(config.ZRangeGapSeconds)*time.Second))
	}()

	for range ticker.C {
		if startTime = startTime.Add(time.Duration(config.ZRangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}

		wg.Add(1)
		go func(startTime time.Time) {
			defer wg.Done()
			w.handleBatch(ctx, minuteBucketKey, startTime, startTime.Add(time.Duration(config.ZRangeGapSeconds)*time.Second))
		}(startTime)
	}

	wg.Wait()

	// 延长锁的过期时间用于真正执行任务
	ack()
	// log.Printf("ack success, key: %s", minuteBucketKey)

	return nil
}

func (w *Worker) handleBatch(ctx context.Context, key string, start, end time.Time) error {
	bucket, err := getBucket(key)
	if err != nil {
		return err
	}

	tasks, err := w.task.GetTasksByTime(ctx, key, bucket, start, end)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		task := task
		if err := w.pool.Submit(func() {
			if err := w.executor.Work(ctx, utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli())); err != nil {
				log.Printf("Executor work failed, err: %v\n", err)
			}
		}); err != nil {
			return err
		}
	}

	return nil
}

func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}

	return utils.GetStartMinute(timeBucket[0])
}

func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}

type taskService interface {
	GetTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*vo.Task, error)
}

type confProvider interface {
	Get() *conf.TriggerAppConf
}
