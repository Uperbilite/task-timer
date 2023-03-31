package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uperbilite/task-timer/common/consts"
)

func UnionTimerIDUnix(timeID uint, unix int64) string {
	return fmt.Sprintf("%d_%d", timeID, unix)
}

func SplitTimerIDUnix(str string) (uint, int64, error) {
	timerIDUnix := strings.Split(str, "_")
	if len(timerIDUnix) != 2 {
		return 0, 0, fmt.Errorf("invalid timerID unix str: %s", str)
	}

	timerID, _ := strconv.ParseInt(timerIDUnix[0], 10, 64)
	unix, _ := strconv.ParseInt(timerIDUnix[1], 10, 64)
	return uint(timerID), unix, nil
}

func GetTaskBloomFilterKey(timeStr string) string {
	return "task_bloom_" + timeStr
}

func GetBucketCntKey(key string) string {
	return "bucket_cnt_" + key
}

func GetTimeBucketLockKey(t time.Time, bucketID int) string {
	return fmt.Sprintf("time_bucket_lock_%s_%d", t.Format(consts.MinuteFormat), bucketID)
}

func GetMigratorLockKey(t time.Time) string {
	return fmt.Sprintf("migrator_lock_%s", t.Format(consts.HourFormat))
}

func GetMonitorLockKey(t time.Time) string {
	return fmt.Sprintf("monitor_lock_%s", t.Format(consts.MinuteFormat))
}

func GetSliceMsgKey(t time.Time, bucketID int) string {
	return fmt.Sprintf("%s_%d", t.Format(consts.MinuteFormat), bucketID)
}

func GetEnableTimerLockKey(app string) string {
	return fmt.Sprintf("enable_timer_lock_%s", app)
}

func GetCreateTimerLockKey(app string) string {
	return fmt.Sprintf("create_timer_lock_%s", app)
}

func SplitTimeBucket(key string) (time.Time, int, error) {
	timerBucket := strings.Split(key, "_")
	if len(timerBucket) != 2 {
		return time.Time{}, 0, fmt.Errorf("invalid time bucket key: %s", key)
	}

	t, err := time.ParseInLocation(consts.MinuteFormat, timerBucket[0], time.Local)
	if err != nil {
		return t, 0, err
	}

	bucket, err := strconv.Atoi(timerBucket[1])
	return t, bucket, err
}

func GetForwardTwoMigrateStepEnd(cur time.Time, diff time.Duration) time.Time {
	end := cur.Add(diff)
	// 向下取整以小时为单位，比如当前时刻是9：55，两小时后是11：55，向下取整为11：00，因此创建的定时任务的区间是9：55到11：00
	return time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), 0, 0, 0, time.Local)
}
