package aaa

import (
	"fmt"
	"math"
)

func Test_struct1() {
	res := func(a, b float64) float64 {
		return math.Pow(a, b)
	}(2, 3)

	fmt.Println("res=", res)

}
