package main

import (
	"fmt"
	"runtime"
)

func main() {
	//channel.Test1()
	cpuNums := runtime.NumCPU()
	fmt.Println("CPU核心数: ", cpuNums)

}
