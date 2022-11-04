package channel

import "fmt"

func TestCh1() {
	var ch1 chan int
	fmt.Printf("%T", ch1)
	ch2 := make(chan bool)

	go func() {
		data, ok := <-ch1
		if ok {
			fmt.Println("子 goroutine 取到数据: ", data)
		}
	}()

	ch1 <- 10
	<-ch2
	fmt.Println("main over...")
}
