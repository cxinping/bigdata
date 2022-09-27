package aaa

import (
	"errors"
	"fmt"
	"math"
)

func Test_error_demo1() {

	TestError1()
}

func TestError1() {
	//res  := math.Sqrt(-100)
	//fmt.Println(res )

	res, err := Sqrt(-100)
	fmt.Println(res, err)
}

func Sqrt(f float64) (float64, error) {
	if f < 0 {
		return 0, errors.New("负数不可以开平方根")
	} else {
		return math.Sqrt(f), nil
	}
}

func TestError2() {
	// recover 例子

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

func Test_type() {
	var str string = "abc"
	fmt.Printf("%T", str)
}
