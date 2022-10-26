package main

import (
	"flag"
	"fmt"
	"runtime"
)

func test1() {
	cpuNums := runtime.NumCPU()
	fmt.Println("CPU核心数: ", cpuNums)

	var numCores = flag.Int("n", 2, "CPU核心数")
	fmt.Println("CPU核心数: ", numCores)
	runtime.GOMAXPROCS(*numCores)

}

func main() {
	//channel.Test1()
	test1()

}
