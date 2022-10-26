package main

import (
	"flag"
	"fmt"
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
		fmt.Println(s)
	}
}

func main() {
	//channel.Test1()
	//showCpuCores()

	go say("world")
	say("hello")

}
