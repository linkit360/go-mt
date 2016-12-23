package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/vostrok/mt_manager/src"
)

func main() {
	c := make(chan os.Signal, 3)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, os.Kill)
	go func() {
		<-c
		src.OnExit()
		os.Exit(1)
	}()

	src.RunServer()
}
