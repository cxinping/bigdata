package main

import "fmt"

func pass_va_val(a int) {
	a++
}

func pass_va_ref(a *int) {
	*a++
}

var a int = 20 /* 声明实际变量 */
func main() {
	//pass_va_val(a)
	pass_va_ref(&a)
	fmt.Printf("a=%d", a)
	//var pa *int = &a
	//
	//fmt.Println(a, *pa)
	//*pa = 5
	//fmt.Println(a, *pa)

}
