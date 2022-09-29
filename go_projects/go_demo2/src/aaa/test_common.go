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

	var c bool = true
	var c2 float32 = 5.3
	fmt.Printf("c= %T, %t , %p \n", c, c, c)
	fmt.Printf("c2= %T, %t , %p \n", c2, c2, c2)

	chinese := 90
	english := 61.5
	avg := (float64(chinese) + english) / 2
	fmt.Println("avg=", avg)

	const NAME string = "wangwu"
	fmt.Printf("%T, %p \n", NAME, NAME)

	const (
		a1 = 2
		a2 = iota
		a3 = 3
		a4 = iota
	)
	fmt.Println(a1, a2, a3, a4)

}

func Test_pointer1() {
	var a int = 5
	var ptr *int

	ptr = &a
	*ptr = 10
	fmt.Printf("a 的值为 %d \n", a)
	fmt.Printf("ptr 的值为 %d %P \n", *ptr, *ptr)

}

func Test_pointer2() {
	a := [4]string{"a", "b", "c", "d"}
	ptr := &a

	fmt.Println(a, ptr)
	ptr[0] = "aaa"
	ptr[1] = "bbb"
	fmt.Println((*ptr)[0], (*ptr)[1])
	fmt.Println(a)

}

var global_name = "wangwu"

func Test_global1() {
	fmt.Println(global_name)
	global_name = "lisi"
	fmt.Println(global_name)
}

func Test_switch1() {
	var x interface{}
	x = "a"
	switch i := x.(type) {
	case nil:
		fmt.Printf("x 的类型 : %T", i)
	case string:
		fmt.Printf("x 的类型 : %T", i)
	default:
		fmt.Printf("未知型")
	}
}

func Test_for1() {
	i := 0
	//for ; ; i++ {
	//	if i > 5 {
	//		break
	//	}
	//	fmt.Printf("i=%d \n", i)
	//}

	for i <= 5 {
		fmt.Printf("i=%d \n", i)
		i++
	}

}

func TestRange() {
	str := "aaaaaa"
	for idx, x := range str {
		fmt.Println(idx, x)
	}
}

func TestChange1() {
	nation := map[string]string{
		"china": "bj",
	}

	fmt.Println(nation)
	modify(nation)
	fmt.Println(nation)
}

func modify(m map[string]string) {
	m["name"] = "wangwu"
}

func TestMap2() {
	m1 := make(map[int]int)
	//chgMap1(m1)
	chgMap2(m1)
	fmt.Println(m1)
}

func chgMap1(m map[int]int) {
	m[1] = 10
	m[2] = 20
}

func chgMap2(m22 map[int]int) {
	fmt.Println("--- chgMap2")
	m22 = make(map[int]int)
	m22[1] = 100
	m22[2] = 200
}

func TestHello1() {
	fmt.Println("hello world")
}
