package aaa

import (
	"fmt"
	"math"
)

type Teacher struct {
	name string
	age  int8
	sex  byte
}

func TestStruct2() {
	var t1 Teacher
	t1.name = "wangwu"
	t1.age = 30
	t1.sex = 1
	fmt.Println(t1)
}

func TestStruct1() {
	//匿名函数
	res := func(a, b float64) float64 {
		return math.Pow(a, b)
	}(2, 3)

	fmt.Println("res=", res)
	fmt.Println("----------------------------------")

	// 匿名类
	addr := struct{ province, city string }{"陕西省", "西安市"}
	fmt.Printf("addr匿名类的类型是 %p", addr)
	fmt.Println(addr.province, addr.city)

}
