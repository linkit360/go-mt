package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/linkit360/go-mt/src"
)

func main() {
	c := make(chan os.Signal, 3)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		src.OnExit()
		os.Exit(1)
	}()

	src.RunServer()
}
