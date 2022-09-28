package aaa

import "fmt"

func TestSwitch1() {
	var x interface{}
	//var x = 10
	switch i := x.(type) {
	case nil:
		fmt.Println("x 的类型是: %T ", i)
	case int:
		fmt.Println("")
	default:
		fmt.Println("未知类型")
	}

}
