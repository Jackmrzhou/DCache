package RaftBased

import (
	"Puzzle/conf"
	pb "Puzzle/idl"
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	config2 "github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"os"
	"time"
)

const (
	ClusterID1 = 1
	ClusterID2 = 2
	ClusterID3 = 3
	ClusterID4 = 4
	// at most four raft groups
)

type RaftBackend struct {
	nh *dragonboat.NodeHost
}

func (rb *RaftBackend) Set(key, value string, ex int64) error  {
	return errors.New("not implemented")
}

func (rb *RaftBackend) Get( key string) (string, error)  {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	res, err := rb.nh.SyncRead(ctx, ClusterID1, []string{"GET", key})
	if err != nil {
		return "", err
	}
	if v, ok := res.(string); ok {
		return v, nil
	}
	return "", errors.New("return type should be redis.StringCmd")
}

func (rb *RaftBackend) HSet(key, field, value string) error {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	cs := rb.nh.GetNoOPSession(ClusterID1)
	proposal := &pb.RaftProposal{Cmds: [][]byte{
		[]byte("HSET"),
		[]byte(key),
		[]byte(field),
		[]byte(value),
	}}
	data, _ := proto.Marshal(proposal)
	_, err := rb.nh.SyncPropose(ctx, cs, data)
	return err
}

func (rb *RaftBackend) HGetAll(key string) (map[string]string, error) {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	res, err := rb.nh.SyncRead(ctx, ClusterID1, []string{"HGETALL", key})
	if err != nil {
		return nil, err
	}
	if v, ok := res.(map[string]string); ok {
		return v, nil
	}
	return nil, errors.New("return type should be map[string]string")
}

func NewRaftBackend(config *conf.Config) *RaftBackend {
	c := config2.Config{
		NodeID:conf.GlobalConf.RaftConf.NodeID,
		ElectionRTT:10,
		HeartbeatRTT:2,
		CheckQuorum:true,
		SnapshotEntries:100,
		CompactionOverhead:50,
	}
	initialMembers := config.RaftConf.InitialMembers
	nodeID := config.RaftConf.NodeID
	dataDir := config.RaftConf.DataDir
	// don't read the old state
	err := os.RemoveAll(dataDir)
	logger.GetLogger("rsm").Warningf("removeAll", err)
	nhc :=config2.NodeHostConfig{
		WALDir:dataDir,
		NodeHostDir:dataDir,
		RTTMillisecond:200,
		RaftAddress: initialMembers[nodeID],
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}

	c.ClusterID = ClusterID1
	if err = nh.StartCluster(initialMembers, false, NewRegionStateMachine, c); err != nil {
		panic(err)
	}

	return &RaftBackend{
		nh:nh,
	}
}