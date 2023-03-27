package scheduler

import (
	"context"
	"time"

	"github.com/uperbilite/task-timer/common/conf"
	"github.com/uperbilite/task-timer/common/utils"
	"github.com/uperbilite/task-timer/pkg/pool"
	"github.com/uperbilite/task-timer/pkg/redis"
	"github.com/uperbilite/task-timer/service/trigger"
)

// 声明成接口
type appConfProvider interface {
	Get() *conf.SchedulerAppConf
}

type lockService interface {
	GetDistributionLock(key string) redis.DistributeLocker
}

type Worker struct {
	pool            pool.WorkerPool
	appConfProvider appConfProvider
	trigger         *trigger.Worker
	lockService     lockService
}

func NewWorker(trigger *trigger.Worker, redisClient *redis.Client, appConfProvider *conf.SchedulerAppConfProvider) *Worker {
	return &Worker{
		pool:            pool.NewGoWorkerPool(appConfProvider.Get().WorkersNum),
		trigger:         trigger,
		lockService:     redisClient,
		appConfProvider: appConfProvider,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.trigger.Start(ctx)

	ticker := time.NewTicker(time.Duration(w.appConfProvider.Get().TryLockGapMilliSeconds) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			// TODO: log
			return nil
		default:
		}

		w.handleSlices(ctx)
	}
	return nil
}

func (w *Worker) handleSlices(ctx context.Context) {
	for i := 0; i < w.getValidBucket(ctx); i++ {
		w.handleSlice(ctx, i)
	}
}

// 禁用动态分桶能力
func (w *Worker) getValidBucket(ctx context.Context) int {
	return w.appConfProvider.Get().BucketsNum
}

func (w *Worker) handleSlice(ctx context.Context, bucketID int) {
	now := time.Now()
	// 为了防止延长锁过期时间失败，开启另一个协程处理前一个分片的任务
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now.Add(-time.Minute), bucketID)
	}); err != nil {
		// TODO: log
	}
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now, bucketID)
	}); err != nil {
		// TODO: log
	}
}

func (w *Worker) asyncHandleSlice(ctx context.Context, t time.Time, bucketID int) {
	// 设置锁过期时间
	locker := w.lockService.GetDistributionLock(utils.GetTimeBucketLockKey(t, bucketID))
	if err := locker.Lock(ctx, int64(w.appConfProvider.Get().TryLockSeconds)); err != nil {
		// TODO: log
		return
	}

	// TODO: log

	// 延长锁过期时间
	ack := func() {
		if err := locker.ExpireLock(ctx, int64(w.appConfProvider.Get().SuccessExpireSeconds)); err != nil {
			// TODO: log
		}
	}

	if err := w.trigger.Work(ctx, utils.GetSliceMsgKey(t, bucketID), ack); err != nil {
		// TODO: log
	}
}
