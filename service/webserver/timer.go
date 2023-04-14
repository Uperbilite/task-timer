package webserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/uperbilite/task-timer/pkg/mysql"
	"sort"
	"time"

	"github.com/uperbilite/task-timer/common/conf"
	"github.com/uperbilite/task-timer/common/consts"
	"github.com/uperbilite/task-timer/common/model/po"
	"github.com/uperbilite/task-timer/common/model/vo"
	"github.com/uperbilite/task-timer/common/utils"
	taskdao "github.com/uperbilite/task-timer/dao/task"
	timerdao "github.com/uperbilite/task-timer/dao/timer"
	"github.com/uperbilite/task-timer/pkg/cron"
	"github.com/uperbilite/task-timer/pkg/redis"
)

const defaultEnableGapSeconds = 3

type TimerService struct {
	timerDAO            timerDAO
	taskDAO             taskDAO
	taskCache           taskCache
	migrateConfProvider *conf.MigratorAppConfProvider
	cronParser          cronParser
	lockService         *redis.Client
}

func NewTimerService(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, taskCache *taskdao.TaskCache, lockService *redis.Client,
	migrateConfProvider *conf.MigratorAppConfProvider, parser *cron.CronParser) *TimerService {
	return &TimerService{
		timerDAO:            timerDAO,
		taskDAO:             taskDAO,
		taskCache:           taskCache,
		migrateConfProvider: migrateConfProvider,
		cronParser:          parser,
		lockService:         lockService,
	}
}

func (t *TimerService) CreateTimer(ctx context.Context, timer *vo.Timer) (uint, error) {
	lock := t.lockService.GetDistributionLock(utils.GetCreateTimerLockKey(timer.App))
	if err := lock.Lock(ctx, defaultEnableGapSeconds); err != nil {
		return 0, errors.New("创建/删除操作过于频繁，请稍后再试！")
	}
	// 校验 cron 表达式
	if !t.cronParser.IsValidCronExpr(timer.Cron) {
		return 0, fmt.Errorf("非法的 cron 表达式: %s", timer.Cron)
	}

	pTimer, err := timer.ToPO()
	if err != nil {
		return 0, err
	}
	return t.timerDAO.CreateTimer(ctx, pTimer)
}

func (t *TimerService) DeleteTimer(ctx context.Context, app string, id uint) error {
	lock := t.lockService.GetDistributionLock(utils.GetCreateTimerLockKey(app))
	if err := lock.Lock(ctx, defaultEnableGapSeconds); err != nil {
		return errors.New("创建/删除操作过于频繁，请稍后再试！")
	}
	return t.timerDAO.DeleteTimer(ctx, id)
}

func (t *TimerService) UpdateTimer(ctx context.Context, timer *vo.Timer) error {
	pTimer, err := timer.ToPO()
	if err != nil {
		return err
	}
	return t.timerDAO.UpdateTimer(ctx, pTimer)
}

func (t *TimerService) GetTimer(ctx context.Context, id uint) (*vo.Timer, error) {
	pTimer, err := t.timerDAO.GetTimer(ctx, timerdao.WithID(id))
	if err != nil {
		return nil, err
	}

	return vo.NewTimer(pTimer)
}

func (t *TimerService) EnableTimer(ctx context.Context, app string, id uint) error {
	// 限制激活和去激活频次
	lock := t.lockService.GetDistributionLock(utils.GetEnableTimerLockKey(app))
	if err := lock.Lock(ctx, defaultEnableGapSeconds); err != nil {
		return errors.New("激活/去激活操作过于频繁，请稍后再试！")
	}

	do := func(ctx context.Context, timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, timer *po.Timer) error {
		// 状态校验
		if timer.Status != consts.Unabled.ToInt() {
			return fmt.Errorf("not unabled status, enable failed, timer id: %d", id)
		}

		// 取得批量的执行时机
		// end 为下一个切片的右边界
		start := time.Now()
		end := utils.GetForwardTwoMigrateStepEnd(start, 2*time.Duration(t.migrateConfProvider.Get().MigrateStepMinutes)*time.Minute)
		executeTimes, err := t.cronParser.NextsBefore(timer.Cron, end)
		if err != nil {
			return err
		}

		// 执行时机加入数据库
		tasks := timer.BatchTasksFromTimer(executeTimes)
		// 基于 timer_id + run_timer 唯一键，保证任务不被重复插入
		// 由于此前可能插入过任务，所以这里忽略重复插入的报错
		if err := taskDAO.BatchCreateTasks(ctx, tasks); err != nil && !mysql.IsDuplicateEntryErr(err) {
			return err
		}

		// 执行时机加入 redis 跳表
		if err := t.taskCache.BatchCreateTasks(ctx, tasks, start, end); err != nil {
			return err
		}

		// 修改 timer 状态为激活态
		timer.Status = consts.Enabled.ToInt()
		return timerDAO.UpdateTimer(ctx, timer)
	}

	return t.timerDAO.DoWithLock(ctx, id, do)
}

func (t *TimerService) UnableTimer(ctx context.Context, app string, id uint) error {
	// 限制激活和去激活频次
	lock := t.lockService.GetDistributionLock(utils.GetEnableTimerLockKey(app))
	if err := lock.Lock(ctx, defaultEnableGapSeconds); err != nil {
		return errors.New("激活/去激活操作过于频繁，请稍后再试！")
	}

	do := func(ctx context.Context, timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, timer *po.Timer) error {
		// 状态校验
		if timer.Status != consts.Enabled.ToInt() {
			return fmt.Errorf("not enabled status, unable failed, timer id: %d", id)
		}

		// 修改 timer 状态为未激活态
		timer.Status = consts.Unabled.ToInt()
		return timerDAO.UpdateTimer(ctx, timer)
	}

	return t.timerDAO.DoWithLock(ctx, id, do)
}

func (t *TimerService) GetAppTimers(ctx context.Context, req *vo.GetAppTimersReq) ([]*vo.Timer, int64, error) {
	total, err := t.timerDAO.CountTimers(ctx, timerdao.WithApp(req.App))
	if err != nil {
		return nil, -1, err
	}

	offset, limit := req.Get()
	if total <= int64(offset) {
		return []*vo.Timer{}, total, nil
	}

	timers, err := t.timerDAO.GetTimers(ctx, timerdao.WithApp(req.App), timerdao.WithPageLimit(offset, limit), timerdao.WithDesc())
	if err != nil {
		return nil, -1, err
	}

	sort.Slice(timers, func(i, j int) bool {
		return timers[i].ID > timers[j].ID
	})

	vTimers, err := vo.NewTimers(timers)
	return vTimers, total, err
}

func (t *TimerService) GetTimersByName(ctx context.Context, req *vo.GetTimersByNameReq) ([]*vo.Timer, int64, error) {
	total, err := t.timerDAO.CountTimers(ctx, timerdao.WithApp(req.App), timerdao.WithFuzzyName(req.FuzzyName))
	if err != nil {
		return nil, -1, err
	}

	offset, limit := req.Get()
	if total <= int64(offset) {
		return []*vo.Timer{}, total, nil
	}

	timers, err := t.timerDAO.GetTimers(ctx, timerdao.WithApp(req.App), timerdao.WithPageLimit(offset, limit), timerdao.WithFuzzyName(req.FuzzyName))
	if err != nil {
		return nil, -1, err
	}

	sort.Slice(timers, func(i, j int) bool {
		return timers[i].ID > timers[j].ID
	})

	vTimers, err := vo.NewTimers(timers)
	return vTimers, total, err
}

type timerDAO interface {
	CreateTimer(ctx context.Context, timer *po.Timer) (uint, error)
	DeleteTimer(ctx context.Context, id uint) error
	UpdateTimer(ctx context.Context, timer *po.Timer) error
	GetTimer(ctx context.Context, opts ...timerdao.Option) (*po.Timer, error)
	GetTimers(ctx context.Context, opts ...timerdao.Option) ([]*po.Timer, error)
	CountTimers(ctx context.Context, opts ...timerdao.Option) (int64, error)
	DoWithLock(ctx context.Context, id uint, do func(ctx context.Context, timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, timer *po.Timer) error) error
}

type taskDAO interface {
	BatchCreateTasks(ctx context.Context, tasks []*po.Task) error
}

type confProvider interface {
	Get() *conf.WebServerAppConf
}

type taskCache interface {
	BatchCreateTasks(ctx context.Context, tasks []*po.Task, start, end time.Time) error
}

type cronParser interface {
	NextsBefore(cron string, end time.Time) ([]time.Time, error)
	IsValidCronExpr(cron string) bool
}
