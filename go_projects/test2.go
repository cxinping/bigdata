package main

import (
	"fmt"
	"io/ioutil"
)

func main() {
	const filename = "D:\\test5\\abc.txt"
	contents, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%s\n", contents)
	}
}
