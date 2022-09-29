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

	//people := RefPeople{name: "wangwu", address: "beijing"}
	//fmt.Printf("RefPeople+> %T %V", people, &people)

	a := 1.5
	fmt.Println(reflect.TypeOf(a))
	fmt.Println(reflect.ValueOf(a))

}
