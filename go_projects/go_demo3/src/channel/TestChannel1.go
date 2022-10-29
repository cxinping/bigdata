package channel

import (
	"fmt"
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

func Test2() {

}
