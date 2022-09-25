package aaa

import (
	"fmt"
	"math"
)

func Test_struct1() {
	//匿名函数
	res := func(a, b float64) float64 {
		return math.Pow(a, b)
	}(2, 3)

	fmt.Println("res=", res)

	// 匿名类
	addr := struct{ province, city string }{"陕西省", "西安市"}
	fmt.Printf("addr匿名类的类型是 %p", addr)
	fmt.Printf("\n-----------------------\n")
	fmt.Println(addr.province, addr.city)

}
