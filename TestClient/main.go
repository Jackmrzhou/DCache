package main

import (
	pb "Puzzle/idl"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"time"
)
const (
	address     = "localhost:9988"
	defaultPayload = "world"
)
func main() {
	fmt.Println("hello world")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal("establish connection failed: " + err.Error())
	}
	c := pb.NewCacheServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.Ping(ctx, &pb.PingRequest{Payload:defaultPayload})
	if err != nil {
		log.Fatal("Ping failed: "+ err.Error())
	}
	log.Println(r.Message)
}
