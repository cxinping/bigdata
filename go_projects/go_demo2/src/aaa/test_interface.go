package aaa

import "fmt"

type Phone interface {
	call()
}

type AndroidPhone struct {
}

func (a AndroidPhone) call() {
	fmt.Println("我是安卓手机，可以打电话")
}

func Test_interface1() {
	var phone Phone
	//phone = new(AndroidPhone)
	//fmt.Printf("%T, %v, %p \n", phone, phone, &phone)
	//phone.call()

	phone = AndroidPhone{}
	fmt.Printf("%T, %v, %p \n", phone, phone, &phone)
	phone.call()
}
