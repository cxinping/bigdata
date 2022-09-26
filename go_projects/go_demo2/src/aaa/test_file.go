package aaa

import (
	"fmt"
	"os"
)

func FileDemo1() {
	fmt.Println(os.ModePerm)
	fmt.Printf("%P ,%T", os.ModePerm, os.ModePerm)
}
