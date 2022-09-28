package aaa

import (
	"fmt"
	"math"
)

type Teacher struct {
	name string
	age  int8
	sex  byte
}

func TestStruct2() {
	var t1 Teacher
	t1.name = "wangwu"
	t1.age = 30
	t1.sex = 1
	fmt.Println(t1)
	fmt.Printf("t1: %p, %v \n", t1, t1)
	var ptr *Teacher = &t1
	(*ptr).name = "lisi"
	fmt.Println(ptr, (*ptr).name, (*ptr).age)
	var t3 = t1
	t3.name = "zhangsan"
	fmt.Println("t3=", t3)
}

func TestStruct1() {
	//匿名函数
	res := func(a, b float64) float64 {
		return math.Pow(a, b)
	}(2, 3)

	fmt.Println("res=", res)
	fmt.Println("----------------------------------")

	// 匿名类
	addr := struct{ province, city string }{"陕西省", "西安市"}
	fmt.Printf("addr匿名类的类型是 %p", addr)
	fmt.Println(addr.province, addr.city)

}

type User struct {
	string
	byte
	int8
	float64
}

func TestStruct3() {
	//结构体的匿名字段
	//user := User{"wang", 'm', 35, 177}
	//fmt.Printf("%v \n", user)
	//fmt.Println(user.string)

	//var t1 = Teacher{name: "wang", age: 35, sex: 1}
	//t2 := Teacher{name: "wang", age: 31, sex: 2}
	//fmt.Printf("t1=> %v \n", t1)
	//fmt.Printf("t2=> %v \n", t2)

	var t3 = new(Teacher)
	//t3.name = "lisi"
	//t3.age = 21
	//t3.sex = 1
	(*t3).name = "lisi"
	(*t3).age = 20
	(*t3).sex = 0
	fmt.Printf("t3=> %v %T\n", t3, t3)
	fmt.Println(t3.name)

}
