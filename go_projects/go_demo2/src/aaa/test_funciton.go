package aaa

import (
	"fmt"
	"strings"
)

func Test_func1() (a, b int) {
	fmt.Println("hello world")
	return 10, 20
}

func TestFunc2() {
	str := "abcdef"
	for i, val := range str {
		fmt.Printf("%d %c \n", i, val)
	}
}

func TestFunAdd(a, b int) int {
	return a + b
}

func TestFunc3(str string) string {
	result := ""
	for i, value := range str {

		if i%2 == 0 {
			result += strings.ToUpper(string(value))
		} else {
			result += strings.ToLower(string(value))
		}
	}
	return result
}

type processFunc func(int) bool

func isEven(integer int) bool {
	// 判断元素是否是偶数
	if integer%2 == 0 {
		return true
	} else {
		return false
	}
}

func isOdd(integer int) bool {
	// 判断元素是否是奇数
	if integer%2 == 0 {
		return false
	} else {
		return true
	}
}

func filter(slice []int, f processFunc) []int {
	var result []int
	for _, value := range slice {
		if f(value) {
			result = append(result, value)
		}
	}
	return result
}

func TestFilter1() {
	slices := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	fmt.Println("slices=", slices)
	odd := filter(slices, isOdd)
	fmt.Println("奇数元素 ", odd)

	even := filter(slices, isEven)
	fmt.Println("偶数元素 ", even)

	fmt.Printf("%v", even)
}
