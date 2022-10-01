package aaa

import (
	"fmt"
	"strconv"
)

func TestArr1() {
	a := [...]int{1, 2, 3}
	//fmt.Printf("%d %v \n", a, a)

	b := a
	b[0] = 10
	fmt.Println(a, b)

	c := [3]int{1, 1, 1}
	changeArr(c)
	fmt.Printf("%v %T", c, c)
	//nums := []int{0, 1, 2}
	//fmt.Println("nums=", nums, len(nums), cap(nums))
}

func changeArr(arr [3]int) {
	arr[0] = 100
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
	fmt.Printf("变量a 地址: %p, 类型: %T, 数值: %v, 长度 %d ，容量 %d \n", &a, a, a, len(a), cap(a))
	fmt.Printf("变量b 地址: %p, 类型: %T, 数值: %v, 长度 %d \n", &b, b, b, len(b))

	c := a
	d := b

	a[1] = 200
	fmt.Println("a=", a, ",c=", c)
	d[0] = 100
	fmt.Println("b=", b, ",d=", d)

}

func TestSlice1() {
	//测试切片
	var sa []string
	//sa := make([]string, 0, 20)
	printSliceMg(sa)
	for i := 0; i < 25; i++ {
		sa = append(sa, strconv.Itoa(i))
		//sa = append(sa, "abc")
		printSliceMg(sa)
	}

}

func printSliceMg(sa []string) {
	fmt.Printf("addr:%p\t len:%v\t cap:%d\t value:%v \n", sa, len(sa), cap(sa), sa)
}

func TestMap1() {
	var country = map[string]string{
		"China": "bj",
		"Japan": "tokyo",
	}

	country2 := country
	fmt.Println(country, country2)
	country2["China"] = "xian"
	fmt.Println(country, country2)

}
