package main

import (
	"fmt"
	"math"
)

func main() {
	var a, b int = 3, 4
	var c int
	c = int(math.Sqrt(float64(a*a) + float64(b*b)))
	fmt.Println(c)
}
