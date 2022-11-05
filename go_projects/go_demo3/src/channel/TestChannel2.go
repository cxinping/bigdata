package channel

import (
	"fmt"
	"time"
)

func TestCh1() {
	//var ch1 chan int
	ch1 := make(chan int)
	//fmt.Printf("%T", ch1)
	ch2 := make(chan bool)

	go func() {
		data, ok := <-ch1
		if ok {
			fmt.Println("子 goroutine 取到数据: ", data)
		}
		ch2 <- true
	}()

	ch1 <- 10
	<-ch2
	fmt.Println("main over...")
}

func TestCh2() {
	ch1 := make(chan int)
	fmt.Println("非缓冲通道: ", len(ch1), cap(ch1))
	go func() {
		ch1 <- 100
		ch1 <- 200
		close(ch1)
		//ch1 <- 300
	}()

	data, ok := <-ch1
	fmt.Println(data, ok)
	data, ok = <-ch1
	fmt.Println(data, ok)
	data, ok = <-ch1
	fmt.Println(data, ok)
}

func TestCh3() {

	//1, 非缓冲通道
	ch1 := make(chan int)
	fmt.Println("非缓冲通道 ch1: ", len(ch1), cap(ch1))

	go func() {
		data := <-ch1
		fmt.Println("收到数据: ", data)
	}()

	ch1 <- 10
	time.Sleep(time.Second)
	fmt.Println("--- 111 main over ---")

	ch2 := make(chan string, 6)
	fmt.Println("非缓冲通道 ch2: ", len(ch2), cap(ch2))

	go sendData2(ch2)
	for data := range ch2 {
		fmt.Println("\t读取数据: ", data)
	}
	fmt.Println("--- 222 main over ---")
}

func sendData2(ch chan string) {
	for i := 0; i < 3; i++ {
		data := fmt.Sprintf("data%d", i)
		ch <- data
		fmt.Println("往通道放入数据: ", data)
	}
	defer close(ch)
}

func TestNewTimer() {
	timer1 := time.NewTimer(3 * time.Second)
	fmt.Println(time.Now())
	data := <-timer1.C
	fmt.Println(data)

}
