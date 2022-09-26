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

}
