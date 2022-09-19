package main

import (
	"fmt"
	"io/ioutil"
)

func test1() {
	fmt.Println("aaa")
}

func main() {
	const filename = "abc.txt"
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%s\n", contents)
	}
}
