package app

import (
	"github.com/uperbilite/task-timer/app/migrator"
	"go.uber.org/dig"

	"github.com/uperbilite/task-timer/app/scheduler"
	"github.com/uperbilite/task-timer/app/webserver"
	"github.com/uperbilite/task-timer/common/conf"
	taskdao "github.com/uperbilite/task-timer/dao/task"
	timerdao "github.com/uperbilite/task-timer/dao/timer"
	"github.com/uperbilite/task-timer/pkg/bloom"
	"github.com/uperbilite/task-timer/pkg/cron"
	"github.com/uperbilite/task-timer/pkg/hash"
	"github.com/uperbilite/task-timer/pkg/mysql"
	"github.com/uperbilite/task-timer/pkg/redis"
	"github.com/uperbilite/task-timer/pkg/xhttp"
	executorservice "github.com/uperbilite/task-timer/service/executor"
	migratorservice "github.com/uperbilite/task-timer/service/migrator"
	schedulerservice "github.com/uperbilite/task-timer/service/scheduler"
	triggerservice "github.com/uperbilite/task-timer/service/trigger"
	webservice "github.com/uperbilite/task-timer/service/webserver"
)

var (
	container *dig.Container
)

func init() {
	container = dig.New()

	provideConfig(container)
	providePKG(container)
	provideDAO(container)
	provideService(container)
	provideApp(container)
}

func provideConfig(c *dig.Container) {
	c.Provide(conf.DefaultMysqlConfProvider)
	c.Provide(conf.DefaultSchedulerAppConfProvider)
	c.Provide(conf.DefaultTriggerAppConfProvider)
	c.Provide(conf.DefaultWebServerAppConfProvider)
	c.Provide(conf.DefaultRedisConfigProvider)
	c.Provide(conf.DefaultMigratorAppConfProvider)
}

func providePKG(c *dig.Container) {
	c.Provide(bloom.NewFilter)
	c.Provide(hash.NewMurmur3Encryptor)
	c.Provide(hash.NewSHA1Encryptor)
	c.Provide(redis.GetClient)
	c.Provide(mysql.GetClient)
	c.Provide(cron.NewCronParser)
	c.Provide(xhttp.NewJSONClient)
}

func provideDAO(c *dig.Container) {
	c.Provide(timerdao.NewTimerDAO)
	c.Provide(taskdao.NewTaskDAO)
	c.Provide(taskdao.NewTaskCache)
}

func provideService(c *dig.Container) {
	c.Provide(migratorservice.NewWorker)
	c.Provide(migratorservice.NewWorker)
	c.Provide(webservice.NewTaskService)
	c.Provide(webservice.NewTimerService)
	c.Provide(executorservice.NewTimerService)
	c.Provide(executorservice.NewWorker)
	c.Provide(triggerservice.NewWorker)
	c.Provide(triggerservice.NewTaskService)
	c.Provide(schedulerservice.NewWorker)
}

func provideApp(c *dig.Container) {
	c.Provide(migrator.NewMigratorApp)
	c.Provide(webserver.NewTaskApp)
	c.Provide(webserver.NewTimerApp)
	c.Provide(webserver.NewServer)
	c.Provide(scheduler.NewWorkerApp)
}

func GetSchedulerApp() *scheduler.WorkerApp {
	var schedulerApp *scheduler.WorkerApp
	if err := container.Invoke(func(_s *scheduler.WorkerApp) {
		schedulerApp = _s
	}); err != nil {
		panic(err)
	}
	return schedulerApp
}

func GetWebServer() *webserver.Server {
	var server *webserver.Server
	if err := container.Invoke(func(_s *webserver.Server) {
		server = _s
	}); err != nil {
		panic(err)
	}
	return server
}

func GetMigratorApp() *migrator.MigratorApp {
	var migratorApp *migrator.MigratorApp
	if err := container.Invoke(func(_m *migrator.MigratorApp) {
		migratorApp = _m
	}); err != nil {
		panic(err)
	}
	return migratorApp
}
