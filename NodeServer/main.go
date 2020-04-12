//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/CacheServer.proto

package main

import (
	pb "Puzzle/idl"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	listenPort = ":9988"
)

type server struct{
	pb.UnimplementedCacheServiceServer
}

func (s *server)Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingResponse, error)  {
	log.Println("Received: " + in.Payload)
	return &pb.PingResponse{Message:"Hello client"}, nil
}
func main() {
	fmt.Println("Hello World")
	lis, err := net.Listen("tcp", listenPort)
	if err != nil {
		log.Fatal("fail to listen: " + err.Error())
	}

	s := grpc.NewServer()
	pb.RegisterCacheServiceServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatal("fail to serve: "+err.Error())
	}
}
