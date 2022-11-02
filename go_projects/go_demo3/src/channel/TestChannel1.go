package main

import (
	"fmt"
	"time"
)

func Test1() {
	ch := make(chan int, 1)
	for {
		select {
		case ch <- 0:
		case ch <- 1:
			break
		}
		i := <-ch
		fmt.Println("接收到的值为: ", i)
		if i == 1 {
			break
		}
	}

}

func hello() {
	fmt.Println("hello world goroutine")
}

func main() {
	//go hello()

	ch1 := make(chan string)
	<-ch1
	//fmt.Println(ch1)
	time.Sleep(2 * time.Second)
	fmt.Println("--- main function over ---")
}
