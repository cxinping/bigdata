package channel

import "fmt"

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
	go func() {
		ch1 <- 100
		ch1 <- 200
		close(ch1)
		ch1 <- 300
	}()

	data, ok := <-ch1
	fmt.Println(data, ok)
	data, ok = <-ch1
	fmt.Println(data, ok)
	data, ok = <-ch1
	fmt.Println(data, ok)
}
