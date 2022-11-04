package main

import (
	"flag"
	"fmt"
	"go_demo3/src/channel"
	"runtime"
)

func showCpuCores() {
	cpuNums := runtime.NumCPU()
	fmt.Println("CPU核心数: ", cpuNums)

	var numCores = flag.Int("n", 2, "CPU核心数")
	fmt.Println("CPU核心数: ", numCores)
	runtime.GOMAXPROCS(*numCores)
	//time.Sleep(5 * time.Second)
}

func say(s string) {
	for i := 0; i < 5; i++ {
		runtime.Gosched()
		fmt.Println(i, s)
	}
}

func main() {
	//channel.Test1()
	//showCpuCores()

	//go say("world")
	//say("hello")

	//time.Sleep(1 * time.Second)

	channel.TestCh1()
}
