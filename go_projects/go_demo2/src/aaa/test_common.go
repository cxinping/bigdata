package aaa

import "fmt"

var Name string = "公有变量"

func Add(a int, b int) int {
	return a + b
}

func Hello() {
	fmt.Println("hello world")
}

func Test_str1() {
	//str := `
	//	aaa
	//	bbb
	//`
	//fmt.Println(str, len(str))

	// 字符
	var a byte = 'a'
	var b rune = '-'

	fmt.Println(a, b)

	var c bool = true
	var c2 float32 = 5.3
	fmt.Printf("c= %T, %t , %p \n", c, c, c)
	fmt.Printf("c2= %T, %t , %p \n", c2, c2, c2)

	chinese := 90
	english := 61.5
	avg := (float64(chinese) + english) / 2
	fmt.Println("avg=", avg)

	const NAME string = "wangwu"
	fmt.Printf("%T, %p \n", NAME, NAME)

	const (
		a1 = 2
		a2 = iota
		a3 = 3
		a4 = iota
	)
	fmt.Println(a1, a2, a3, a4)

}

func Test_pointer1() {
	var a int = 5
	var ptr *int

	ptr = &a
	*ptr = 10
	fmt.Printf("a 的值为 %d \n", a)
	fmt.Printf("ptr 的值为 %d %P \n", *ptr, *ptr)

}

var global_name = "wangwu"

func Test_global1() {
	fmt.Println(global_name)
	global_name = "lisi"
	fmt.Println(global_name)
}
