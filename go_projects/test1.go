package main

import "fmt"

func variableZeroValue() {
	var a int
	var s string
	fmt.Printf("%d %q\n", a, s)

}

func variableInitValue() {
	var a, b int = 3, 4
	var s string = "abc"

	fmt.Println(a, b, s)
}

func main() {
	//variableZeroValue()
	variableInitValue()
}
