package main

import "fmt"

func main() {
	var a int = 20 /* 声明实际变量 */
	//var ip *int    /* 声明指针变量 */
	var team [3]string
	team[0] = "a"
	team[1] = "b"
	team[2] = "c"

	//ip = &a /* 指针变量的存储地址 */

	fmt.Printf("a 变量的地址是: %x\n", &a, &team, &team[0], &team[1])

	///* 指针变量的存储地址 */
	//fmt.Printf("ip 变量储存的指针地址: %x\n", ip)
	//
	///* 使用指针访问值 */
	//fmt.Printf("*ip 变量的值: %d\n", *ip)

}
