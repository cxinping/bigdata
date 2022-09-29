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

	//测试数组
	//aaa.TestArr1()
	//aaa.Arr2()
	//aaa.Slice1()
	//aaa.TestSlice1()
	//aaa.TestMap1()
	//aaa.Arr3()
	//aaa.Arr4()

	//异常处理
	//aaa.TestError1()

	// 面向对象
	//aaa.TestStruct1()
	//aaa.TestStruct2()
	//aaa.TestStruct3()

	// Go语言的流程控制
	//aaa.TestSwitch1()

	//接口测试用例
	//aaa.Test_interface1()
	//aaa.Test_interface2()
	//aaa.FunA()

	//指针
	//aaa.Test_pointer2()

	//aaa.Test_error_demo1()

	//aaa.Test_type()
	///aaa.FileDemo1()

	//aaa.TestConnDB()

	//aaa.MapToJson()

	//aaa.Test_str1()
	//aaa.Test_pointer1()
	///aaa.Test_global1()

	aaa.Test_switch1()
	//aaa.Test_for1()

	//面向对象编程
	//_, a := aaa.TestFunc1()
	//fmt.Println(a)
	//aaa.TestFunc2()
	//aaa.TestFunc5()

	//rst := aaa.TestFunAdd(3, 4)
	//fmt.Println(rst)
	//fmt.Printf("rst的数据类型是 %T", rst)

	//rst := aaa.TestFunc3("abcdef")
	//fmt.Printf("%P", rst)

	//aaa.TestFilter1()
	//aaa.TestFunc4()
	//aaa.TestPointer1()
	//aaa.TestPointer2()

	//aaa.TestWeb1()
}
