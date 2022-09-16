package main

import "fmt"

type weapon int

const (
	ARROW weapon = iota
	Shriken
	Sniper
)

func main() {
	fmt.Println("hello Go")
	fmt.Println(ARROW, Shriken, Sniper)
}
