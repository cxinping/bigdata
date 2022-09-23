package main

import "fmt"

type Rectangle struct {
	width, height float64
}

func demo1() {
	r1 := Rectangle{5, 9}
	fmt.Printf("r1的地址： %p \n", &r1)
	var i, j = 5, 6
	fmt.Printf("i的地址： %p %p \n", &i, &j)
}

func main() {
	//fmt.Println("hello world")

	demo1()

}
