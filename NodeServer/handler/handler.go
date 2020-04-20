package handler

import (
	"Puzzle/NodeServer/utils"
	"Puzzle/Storage"
	pb "Puzzle/idl"
	"context"
	"encoding/binary"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type Handler struct {
	pb.UnimplementedCacheServiceServer
	*Storage.StorageService
}

func (s *Handler) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	log.Println("Received: " + req.Payload)
	return &pb.PingResponse{Message:"Hello client"}, nil
}

func (s *Handler) SetValues(ctx context.Context, req *pb.SetValuesRequest) (*pb.SetValuesResponse, error) {
	for _, cell := range req.Cells {
		log.Println("Add cell for:" + string(cell.Row))
		// encode here
		timestampBytes := make([]byte, 8)
		typeBytes := make([]byte, 4)
		binary.LittleEndian.PutUint64(timestampBytes, uint64(cell.Timestamp))
		binary.LittleEndian.PutUint32(typeBytes, uint32(cell.Type))
		merged := append(timestampBytes, typeBytes...)
		err := s.HSet(string(append(cell.Row, cell.ColumnFamily...)), string(cell.Column), string(append(cell.Value, merged...)))
		if err != nil {
			//todo:rollback all the set values
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &pb.SetValuesResponse{Code:0, Message:"ok"}, nil
}

func (s *Handler) GetRow(ctx context.Context, req *pb.GetRowRequest) (*pb.GetRowResponse, error) {
	log.Println("Get row for key:" + string(req.Key))
	cfMap := utils.GetShortCfMapCopy()
	result := make([]*pb.HCell, 0)
	for _, v := range cfMap{
		res, err := s.HGetAll(string(req.Key)+v)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if len(res) != 0 {
			for col, val := range res {
				val := []byte(val)
				timestampBytes := val[len(val) - 12: len(val) - 4]
				typeBytes := val[len(val)-4:]
				result = append(result, &pb.HCell{
					Row:req.Key,
					ColumnFamily: []byte(v),
					Column:[]byte(col),
					Value:val[0: len(val) - 12],
					Timestamp: int64(binary.LittleEndian.Uint64(timestampBytes)),
					Type:int32(binary.LittleEndian.Uint32(typeBytes)),
				})
			}
		}
	}
	if len(result) == 0 {
		return &pb.GetRowResponse{Code:1, Result:nil, Message:"no records in cache"}, nil
	}
	return &pb.GetRowResponse{Code:0, Result:result, Message:"ok"}, nil
}