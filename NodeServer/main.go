//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/CacheServer.proto
package main

import (
	"Puzzle/NodeServer/handler"
	"Puzzle/Storage"
	"Puzzle/conf"
	pb "Puzzle/idl"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	listenPort = ":9988"
)

func main() {
	config, err := conf.LoadConf("")
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", listenPort)
	if err != nil {
		log.Fatal("fail to listen: " + err.Error())
	}
	storageService := Storage.NewStorageService(config)
	s := grpc.NewServer()
	h := &handler.Handler{StorageService:storageService}
	pb.RegisterCacheServiceServer(s, h)
	log.Println("starting service")
	if err := s.Serve(lis); err != nil {
		log.Fatal("fail to serve: "+err.Error())
	}
}
