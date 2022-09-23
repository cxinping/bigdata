package main

import "fmt"

//func main() {
//	/* 创建切片 */
//	//numbers := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
//	//printSlice(numbers)
//
//	var numbers []int
//	printSlice(numbers)
//
//	/* 允许追加空切片 */
//	numbers = append(numbers, 0)
//	printSlice(numbers)
//
//	/* 向切片添加一个元素 */
//	numbers = append(numbers, 1)
//	printSlice(numbers)
//
//	/* 同时添加多个元素 */
//	numbers = append(numbers, 2, 3, 4)
//	printSlice(numbers)
//
//}

func printSlice(x []int) {
	fmt.Printf("len=%d cap=%d slice=%v\n", len(x), cap(x), x)
}





