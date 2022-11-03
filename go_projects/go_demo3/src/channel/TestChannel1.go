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

func recvData(ch1 chan string) {

	rst1, ok := <-ch1
	//rst2, ok := <-ch1
	fmt.Println("rst1=", rst1, ok)
	//fmt.Println("rst2=", rst2, ok)
}

func main() {
	//go hello()

	ch1 := make(chan string)
	go recvData(ch1)

	go sendData(ch1)

	time.Sleep(1 * time.Second)
	fmt.Println("--- main function over ---")
}
