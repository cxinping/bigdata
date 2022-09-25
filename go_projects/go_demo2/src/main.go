package main

import (
	"aaa"
	"database/sql"
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

var (
	userName  string = "xinping"
	password  string = "123456"
	ipAddrees string = "192.168.11.11"
	port      int    = 3306
	dbName    string = "codebaoku"
	charset   string = "utf8"
)

func test_db() {
	db, err := sql.Open("mysql", "xinping:123456@/codebaoku?charset=utf8")
	fmt.Println(db, err)

}

func main() {
	//fmt.Println("hello world 222")

	//demo1()

	//test_db()

	//demo2()

	//aaa.Hello()
	//fmt.Println(aaa.Add(1, 2))
	//fmt.Println(aaa.Name)
	//aaa.Arr1()

	//aaa.Arr2()
	//aaa.Slice1()

	//aaa.Arr3()
	aaa.Arr4()

}
