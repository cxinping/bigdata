package main

import (
	"aaa"
	"fmt"
)

type Rectangle struct {
	width, height float64
}

func demo1() {
	r1 := Rectangle{5, 9}
	fmt.Printf("r1的地址： %p \n", &r1)
	var i, j = 5, 6
	fmt.Printf("i的地址： %p %p \n", &i, &j)
}

func demo2() {
	fmt.Println(1/2, 1.0/2.0)

	var nums = [...]int{1, 2, 3}
	fmt.Println(nums)
}

func main() {
	//fmt.Println("hello world 222")

	//demo1()

	//demo2()

	//aaa.Hello()
	//fmt.Println(aaa.Add(1, 2))
	//fmt.Println(aaa.Name)
	//aaa.Arr1()

	//aaa.Arr2()
	//aaa.Slice1()

	//aaa.Arr3()
	//aaa.Arr4()
	//aaa.Test_struct1()

	//接口测试用例
	//aaa.Test_interface1()
	//aaa.Test_interface2()
	//aaa.FunA()

	//aaa.Test_error_demo1()

	//aaa.Test_type()
	///aaa.FileDemo1()

	//aaa.TestConnDB()

	//aaa.MapToJson()

	//aaa.Test_str1()
	//aaa.Test_pointer1()
	///aaa.Test_global1()

	//aaa.Test_switch1()
	//aaa.Test_for1()

	//_, a := aaa.Test_func1()
	//fmt.Println(a)
	//aaa.TestFunc2()

	//rst := aaa.TestFunAdd(3, 4)
	//fmt.Println(rst)
	//fmt.Printf("rst的数据类型是 %T", rst)

	//rst := aaa.TestFunc3("abcdef")
	//fmt.Printf("%P", rst)

	//aaa.TestFilter1()
	//aaa.TestFunc4()
	aaa.TestPointer1()

	//aaa.TestWeb1()
}
