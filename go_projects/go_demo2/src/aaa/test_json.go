package aaa

import (
	"encoding/json"
	"fmt"
)

func MapToJson() {
	// 实现map转JSON
	m := map[string][]string{
		"level":   {"debug"},
		"message": {"File not Found"},
	}

	m2 := map[string]string{
		"level":   "INFO",
		"message": "it is ok",
	}

	fmt.Println(m, m2)

	if data, err := json.Marshal(m); err == nil {
		fmt.Printf("%s\n", data)
	}

	if data, err := json.Marshal(m2); err == nil {
		fmt.Printf("%s\n", data)
	}

	var str1 string = "111" + "222"
	fmt.Printf(str1)
}
