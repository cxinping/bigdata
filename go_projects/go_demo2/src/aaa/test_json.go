package aaa

import (
	"encoding/json"
	"fmt"
)

func TestMapToJson1() {
	// 实现map转JSON
	m := map[string][]string{
		"level":   {"debug", "info"},
		"message": {"File not Found", "error1", "error2"},
	}

	//m2 := map[string]string{
	//	"level":   "INFO",
	//	"message": "it is ok",
	//}

	//fmt.Println(m, m2)

	if data, err := json.Marshal(m); err == nil {
		fmt.Printf("%T, %s\n", data, data)
	}

	fmt.Println("-------------------------")
	if data, err := json.MarshalIndent(m, "", " "); err == nil {
		fmt.Printf("%T, %s\n", data, data)
	}

	//if data, err := json.Marshal(m2); err == nil {
	//	fmt.Printf("%s\n", data)
	//}
	//
	//var str1 string = "111" + "222"
	//fmt.Printf(str1)
}

type JsonDebugInfo struct {
	Level  string
	Msg    string
	Author string
}

func TestMapToJson2() {
	dbgInfos := []JsonDebugInfo{JsonDebugInfo{Level: "info", Msg: "msg1", Author: "wangwu"}, JsonDebugInfo{Level: "debug", Msg: "msg2", Author: "lisi"}}
	fmt.Println(dbgInfos, len(dbgInfos), cap(dbgInfos))

	data, err := json.Marshal(dbgInfos)
	if err == nil {
		fmt.Printf("data type => %T, data => %s\n", data, data)
		fmt.Printf("data 类型 => %T , 详情 => %v", string(data), string(data))
	}
}

type JsonUser struct {
	Name    string `json:"_name""`
	Age     int    `json:"_age" `
	Sex     uint   `json:"-"`
	Address string //不改变标签
}

func TestMapToJson3() {
	//结构体字段标签
	user := JsonUser{Name: "wangwu", Age: 21, Sex: 1, Address: "北京海淀"}
	arr, error := json.Marshal(user)
	if error == nil {
		fmt.Printf("%T , %v\n", string(arr), string(arr))
	}

	var nums = []int{1, 2, 3, 4}
	fmt.Printf("%T %v\n", nums, nums)

}
