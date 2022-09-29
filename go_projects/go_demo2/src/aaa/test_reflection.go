package aaa

import (
	"fmt"
	"reflect"
)

type RefPeople struct {
	name    string
	address string
}

func TestReflection1() {
	fmt.Println("--- over ---")

	//a := 1.5
	//fmt.Println(reflect.TypeOf(a))
	//fmt.Println(reflect.ValueOf(a))

	people := RefPeople{name: "wangwu", address: "北京海淀"}
	//fmt.Printf("RefPeople+> %T %V", people, &people)

	v := reflect.ValueOf(people)
	fmt.Println(v.NumField(), v.FieldByIndex([]int{0}), v.FieldByIndex([]int{1}))

}
