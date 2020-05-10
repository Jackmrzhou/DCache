//go:generate protoc -I idl --go_out=plugins=grpc:idl idl/CacheServer.proto idl/raft.proto
package main

import (
	"Puzzle/NodeServer/handler"
	"Puzzle/Storage"
	"Puzzle/conf"
	pb "Puzzle/idl"
	"fmt"
	"github.com/lni/dragonboat/v3/logger"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// todo:clear cache on start up
	config, err := conf.LoadConf("")
	if err != nil {
		log.Fatal(err)
	}
	lis, err := net.Listen("tcp", ":"+config.ServicePort)
	if err != nil {
		log.Fatal("fail to listen: " + err.Error())
	}
	storageService := Storage.NewStorageService(config)
	s := grpc.NewServer()
	h := &handler.Handler{StorageService:storageService}
	pb.RegisterCacheServiceServer(s, h)
	log.Println("starting service")

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("recovered panic in main:", r)
		}
		storageService.Close()
	}()

	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal("fail to serve: " + err.Error())
		}
	}()
	<- stopChan
	s.GracefulStop()
}
