package aaa

import "fmt"

func Test_error_demo1() {
	// recover 例子
	FunA()
}

func FunA() {
	fmt.Println("这是funcA begin")
	defer func() {
		if msg := recover(); msg != nil {
			fmt.Println("恢复啦，获取recover的返回值:", msg)
		}
	}()

	for i := 0; i < 10; i++ {
		fmt.Println("i=", i)
		if i == 5 {
			panic("* FuncA 发生恐慌")
		}
	}
	fmt.Println("这是funcA end")

}
