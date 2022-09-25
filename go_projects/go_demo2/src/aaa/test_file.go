package aaa

import (
	"fmt"
	"os"
)

func FileDemo1() {
	fmt.Println(os.ModePerm)
	fmt.Printf("%p", os.ModePerm)
}
