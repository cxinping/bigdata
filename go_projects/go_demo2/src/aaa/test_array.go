package aaa

import "fmt"

func Arr1() {
	nums := []int{0, 1, 2}
	fmt.Println("nums=", nums, len(nums), cap(nums))
}

func Arr2() {
	a := [...]string{"a", "b", "c"}
	b := a

	fmt.Println(a)
	b[0] = "aaa"
	fmt.Println(b)
}

func Arr3() {
	a := [4]float64{167.7, 59.8, 78}
	b := []int{2, 3, 4}
	fmt.Printf("变量a 地址: %p, 类型: %T, 数值: %v, 长度 %d \n", &a, a, a, len(a))
	fmt.Printf("变量b 地址: %p, 类型: %T, 数值: %v, 长度 %d \n", &b, b, b, len(b))

	c := a
	d := b

	a[1] = 200
	fmt.Println("a=", a, ",c=", c)
	d[0] = 100
	fmt.Println("b=", b, ",d=", d)

}
