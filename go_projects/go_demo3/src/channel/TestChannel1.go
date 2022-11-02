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

func sendData(ch1 chan string) {
	ch1 <- "a"
	ch1 <- "b"
}

func main() {
	//go hello()

	ch1 := make(chan string)
	go sendData(ch1)
	rst1 := <-ch1
	rst2 := <-ch1
	fmt.Println(rst1, rst2)
	time.Sleep(1 * time.Second)
	fmt.Println("--- main function over ---")
}
