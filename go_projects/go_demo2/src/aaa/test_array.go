package aaa

import "fmt"

func Arr1() {
	nums := []int{0, 1, 2}
	fmt.Println("nums=", nums, len(nums), cap(nums))
}
