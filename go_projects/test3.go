package main

import "fmt"

func main() {
	var a int = 20 /* 声明实际变量 */
	var pa *int = &a

	fmt.Println(a, *pa)
	*pa = 5
	fmt.Println(a, *pa)

}
