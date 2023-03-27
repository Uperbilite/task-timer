package main

import (
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/uperbilite/task-timer/app"
)

func main() {
	schedulerApp := app.GetSchedulerApp()
	webServer := app.GetWebServer()

	schedulerApp.Start()
	defer schedulerApp.Stop()

	webServer.Start()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT)
	<-quit
}
