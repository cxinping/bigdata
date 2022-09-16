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

func variableTypeDuction() {
	var a, b, c, s = 3, 4, true, "def"
	fmt.Println(a, b, c, s)
}

func variableShorter() {
	a, b, c, s := 3, 4, true, "def"
	b = 5
	fmt.Println(a, b, c, s)
}

func main() {
	//variableZeroValue()
	//variableInitValue()
	//variableTypeDuction()
	variableShorter()

}
