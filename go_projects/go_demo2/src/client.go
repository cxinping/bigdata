package main

import (
	"fmt"
	"log"
	"net/rpc"
	//"time"
)

func testClient() {

	type Args struct {
		A, B int
	}

	type Quotient struct {
		Quo, Rem int
	}

	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call
	//args := &Args{3, 7}
	//var reply int
	//err = client.Call("Arith.Multiply", args, &reply)
	//if err != nil {
	//	log.Fatal("arith error:", err)
	//}
	//fmt.Printf("Arith: %d*%d=%d", args.A, args.B, reply)

	// Asynchronous call
	args := &Args{10, 3}
	quotient := new(Quotient)
	divCall := client.Go("Arith.Divide", args, quotient, nil)
	replyCall := <-divCall.Done // will be equal to divCall
	// check errors, print, etc.
	fmt.Println(replyCall.Reply)

}

func main() {
	testClient()
}
