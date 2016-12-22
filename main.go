package main

import (
	"github.com/vostrok/mt_manager/src"
)

func main() {
	defer src.OnExit()
	src.RunServer()
}
