package service

import (
	"context"
	"log"
	"time"

	mconf "github.com/uperbilite/task-timer/common/conf"
	"github.com/uperbilite/task-timer/common/consts"
	"github.com/uperbilite/task-timer/common/utils"
	taskdao "github.com/uperbilite/task-timer/dao/task"
	timerdao "github.com/uperbilite/task-timer/dao/timer"
	"github.com/uperbilite/task-timer/pkg/cron"
	"github.com/uperbilite/task-timer/pkg/pool"
	"github.com/uperbilite/task-timer/pkg/redis"
)

type Worker struct {
	timerDAO          *timerdao.TimerDAO
	taskDAO           *taskdao.TaskDAO
	taskCache         *taskdao.TaskCache
	cronParser        *cron.CronParser
	lockService       *redis.Client
	appConfigProvider *mconf.MigratorAppConfProvider
	pool              pool.WorkerPool
}

func NewWorker(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, taskCache *taskdao.TaskCache, lockService *redis.Client,
	cronParser *cron.CronParser, appConfigProvider *mconf.MigratorAppConfProvider) *Worker {
	return &Worker{
		pool:              pool.NewGoWorkerPool(appConfigProvider.Get().WorkersNum),
		timerDAO:          timerDAO,
		taskDAO:           taskDAO,
		taskCache:         taskCache,
		lockService:       lockService,
		cronParser:        cronParser,
		appConfigProvider: appConfigProvider,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	conf := w.appConfigProvider.Get()
	ticker := time.NewTicker(time.Duration(conf.MigrateStepMinutes) * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("migrator ticking...")
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// 保证同一时刻只有一个机器执行迁移过程
		locker := w.lockService.GetDistributionLock(utils.GetMigratorLockKey(utils.GetStartHour(time.Now())))
		if err := locker.Lock(ctx, int64(conf.MigrateTryLockMinutes)*int64(time.Minute/time.Second)); err != nil {
			log.Printf("migrator get lock failed, key: %s, err: %v", utils.GetMigratorLockKey(utils.GetStartHour(time.Now())), err)
			continue
		}

		if err := w.migrate(ctx); err != nil {
			log.Printf("migrate failed, err: %v", err)
			continue
		}

		// 每隔两小时执行一次迁移过程
		_ = locker.ExpireLock(ctx, int64(conf.MigrateSuccessExpireMinutes)*int64(time.Minute/time.Second))
	}
	return nil
}

func (w *Worker) migrate(ctx context.Context) error {
	timers, err := w.timerDAO.GetTimers(ctx, timerdao.WithStatus(int32(consts.Enabled.ToInt())))
	if err != nil {
		return err
	}

	conf := w.appConfigProvider.Get()
	now := time.Now()
	start := utils.GetStartHour(now.Add(time.Duration(conf.MigrateStepMinutes) * time.Minute))
	end := utils.GetStartHour(now.Add(2 * time.Duration(conf.MigrateStepMinutes) * time.Minute))
	// 迁移可以慢慢来，不着急
	for _, timer := range timers {
		nexts, _ := w.cronParser.NextsBetween(timer.Cron, start, end)
		if err := w.taskDAO.BatchCreateTasks(ctx, timer.BatchTasksFromTimer(nexts)); err != nil {
			log.Printf("migrator batch create records for timer: %d failed, err: %v", timer.ID, err)
			return err
		}
		time.Sleep(5 * time.Second)
	}

	return w.migrateToCache(ctx, start, end)
}

func (w *Worker) migrateToCache(ctx context.Context, start, end time.Time) error {
	// 迁移完成后，将所有添加的 task 取出，添加到 redis 当中
	tasks, err := w.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		log.Printf("migrator batch create cache failed, err: %v", err)
		return err
	}
	return w.taskCache.BatchCreateTasks(ctx, tasks, start, end)
}
