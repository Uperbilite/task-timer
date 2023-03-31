package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/uperbilite/task-timer/pkg/prometheus"
	nethttp "net/http"
	"strings"
	"time"

	"github.com/uperbilite/task-timer/common/consts"
	"github.com/uperbilite/task-timer/common/model/vo"
	"github.com/uperbilite/task-timer/common/utils"
	taskdao "github.com/uperbilite/task-timer/dao/task"
	"github.com/uperbilite/task-timer/pkg/bloom"
	"github.com/uperbilite/task-timer/pkg/xhttp"
)

type Worker struct {
	timerService *TimerService
	taskDAO      *taskdao.TaskDAO
	httpClient   *xhttp.JSONClient
	bloomFilter  *bloom.Filter
	reporter     *prometheus.Reporter
}

func NewWorker(timerService *TimerService, taskDAO *taskdao.TaskDAO, httpClient *xhttp.JSONClient, bloomFilter *bloom.Filter, reporter *prometheus.Reporter) *Worker {
	return &Worker{
		timerService: timerService,
		taskDAO:      taskDAO,
		httpClient:   httpClient,
		bloomFilter:  bloomFilter,
		reporter:     reporter,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.timerService.Start(ctx)
}

func (w *Worker) Work(ctx context.Context, timerIDUnixKey string) error {
	// 拿到消息，查询一次完整的 timer 定义
	timerID, unix, err := utils.SplitTimerIDUnix(timerIDUnixKey)
	if err != nil {
		return err
	}

	if exist, err := w.bloomFilter.Exist(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), timerIDUnixKey); err != nil || exist {
		// 查库判断定时器状态
		task, err := w.taskDAO.GetTask(ctx, taskdao.WithTimerID(timerID), taskdao.WithRunTimer(time.UnixMilli(unix)))
		if err == nil && task.Status != consts.NotRunned.ToInt() {
			// 重复执行的任务
			return nil
		}
	}

	return w.executeAndPostProcess(ctx, timerID, unix)
}

func (w *Worker) executeAndPostProcess(ctx context.Context, timerID uint, unix int64) error {
	// 未执行，则查询 timer 完整的定义，执行回调
	timer, err := w.timerService.GetTimer(ctx, timerID)
	if err != nil {
		return fmt.Errorf("get timer failed, id: %d, err: %w", timerID, err)
	}

	// 定时器已经处于去激活态，则无需处理任务
	if timer.Status != consts.Enabled {
		return nil
	}

	// 获得实际执行时间，用于计算任务执行延时
	execTime := time.Now()
	resp, err := w.execute(ctx, timer)

	return w.postProcess(ctx, resp, err, timer.App, timerID, unix, execTime)
}

func (w *Worker) execute(ctx context.Context, timer *vo.Timer) (map[string]interface{}, error) {
	var (
		resp map[string]interface{}
		err  error
	)
	switch strings.ToUpper(timer.NotifyHTTPParam.Method) {
	case nethttp.MethodGet:
		err = w.httpClient.Get(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, nil, &resp)
	case nethttp.MethodPatch:
		err = w.httpClient.Patch(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodDelete:
		err = w.httpClient.Delete(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodPost:
		err = w.httpClient.Post(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	default:
		err = fmt.Errorf("invalid http method: %s, timer: %s", timer.NotifyHTTPParam.Method, timer.Name)
	}

	return resp, err
}

func (w *Worker) postProcess(ctx context.Context, resp map[string]interface{}, execErr error, app string, timerID uint, unix int64, execTime time.Time) error {
	go w.reportMonitorData(app, unix, execTime)

	w.bloomFilter.Set(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), utils.UnionTimerIDUnix(timerID, unix), consts.BloomFilterKeyExpireSeconds)

	task, err := w.taskDAO.GetTask(ctx, taskdao.WithTimerID(timerID), taskdao.WithRunTimer(time.UnixMilli(unix)))
	if err != nil {
		return fmt.Errorf("get task failed, timerID: %d, runTimer: %d, err: %w", timerID, time.UnixMilli(unix), err)
	}

	respBody, _ := json.Marshal(resp)
	task.Output = string(respBody)

	if execErr != nil {
		task.Status = consts.Failed.ToInt()
	} else {
		task.Status = consts.Successed.ToInt()
	}

	return w.taskDAO.UpdateTask(ctx, task)
}

func (w *Worker) reportMonitorData(app string, expectExecTimeUnix int64, actualExecTime time.Time) {
	w.reporter.ReportExecRecord(app)
	// 上报毫秒
	w.reporter.ReportTimerDelayRecord(app, float64(actualExecTime.UnixMilli()-expectExecTimeUnix))
}
