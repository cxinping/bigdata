package aaa

import (
	"fmt"
	"time"
)

func TestRountine1() {
	go running1()

	//var input string
	//fmt.Scanln(&input)
	time.Sleep(5 * time.Second)
	fmt.Println("--- main over ---")
}

func TestRountine2() {
	//num := runtime.GOMAXPROCS(6)
	//fmt.Println("num=", num)

	aaa := fmt.Sprintf("a%d", 10)
	fmt.Println(aaa)
	fmt.Printf("%q, %v, %T", aaa, aaa, aaa)

	var ch1 chan int
	var ch2 = make(chan int)
	fmt.Printf("ch1 %T, %v\n", ch1, ch1)
	fmt.Printf("ch2 %T, %v\n", ch2, ch2)

}

func running1() {
	var times int
	for i := 0; i < 10; i++ {
		times++
		fmt.Println("running1 tick=> ", times)
		time.Sleep(1 * time.Second)
	}
}

func running2() {
	var times int
	for {
		times++
		fmt.Println("running2 tick=> ", times)
		time.Sleep(1 * time.Second)
	}
}

func TestRountine3() {
	ch1 := make(chan int)
	go func() {
		ch1 <- 100
		ch1 <- 200
		close(ch1)

		ch1 <- 300
	}()

	data, ok := <-ch1
	fmt.Println("1 main 读取数据: ", data, ok)
	data, ok = <-ch1
	fmt.Println("2 main 读取数据: ", data, ok)
	data, ok = <-ch1
	fmt.Println("3 main 读取数据: ", data, ok)
	data, ok = <-ch1
	fmt.Println("4 main 读取数据: ", data, ok)
	data, ok = <-ch1
	fmt.Println("5 main 读取数据: ", data, ok)

}

func TestRountine4() {

}
