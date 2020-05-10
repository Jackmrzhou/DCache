package RaftBased

import (
	"Puzzle/conf"
	pb "Puzzle/idl"
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	config2 "github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ClusterID1 = 1
	ClusterID2 = 2
	ClusterID3 = 3
	ClusterID4 = 4
	// at most four raft groups
	RowSet = "RowSet"
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
	if err != nil && strings.Contains(err.Error(), "OOM") {
		// out of memory

	}
	return err
}

func (rb *RaftBackend) HMSet(key string, fields map[string]interface{}) error {
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	cs := rb.nh.GetNoOPSession(ClusterID1)
	buf := new(bytes.Buffer)
	encode := gob.NewEncoder(buf)
	err := encode.Encode(fields)
	if err != nil {
		return err
	}
	proposal := &pb.RaftProposal{Cmds: [][]byte{
		[]byte("HMSET"),
		[]byte(key),
		buf.Bytes(),
	}}
	data, _ := proto.Marshal(proposal)
	_, err = rb.nh.SyncPropose(ctx, cs, data)
	if err != nil && strings.Contains(err.Error(), "OOM") {
		// out of memory
		var keysToDel []string
		keysToDel = append(keysToDel, key[:len(key)-1])
		// get row keys to be deleted
		ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
		res, err := rb.nh.SyncRead(ctx, ClusterID1, []string{"ZCARD", RowSet})
		if err != nil {
			return err
		}
		end := res.(int64) / 3

		shortCfs := []string{"g", "h", "i", "e", "f", "s", "t", "m", "l"}

		for start := int64(0); start < end; start += 10000 {
			ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
			count := int64(10000)
			if start + count > end {
				count = end - start
			}
			res, err = rb.nh.SyncRead(ctx, ClusterID1, []interface{}{"ZRANGE", RowSet, 0, count})
			if err != nil {
				return err
			}
			// delete associated hash table from redis
			ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
			cmd := make([][]byte, len(res.([]string)) * len(shortCfs) + 1)
			cmd[0] = []byte("DEL")
			// todo: set deleted in case when part of a row is deleted, and the rest deletions failed
			for i, val :=range res.([]string) {
				for j, cf := range shortCfs {
					cmd[1+i*j] = []byte(val + cf)
				}
			}
			proposal = &pb.RaftProposal{Cmds: [][]byte{}}
			data, _ := proto.Marshal(proposal)
			_, err = rb.nh.SyncPropose(ctx, cs, data)
			if err != nil {
				return err
			}
			// delete from sorted set
			ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
			proposal = &pb.RaftProposal{Cmds: [][]byte{
				[]byte("ZREMRANGEBYRANK"),
				[]byte(RowSet),
				[]byte("0"),
				[]byte(strconv.FormatInt(end, 10)),
			}}
			data, _ = proto.Marshal(proposal)
			_, err = rb.nh.SyncPropose(ctx, cs, data)
			if err != nil {
				return err
			}
		}
	}

	if err != nil {
		// set fields failed
		return err
	}

	// update rank
	// failure is ignored here as it's insignificant

	return nil
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

func (rb *RaftBackend) Close() error {
	rb.nh.Stop()
	return nil
}

func NewRaftBackend(config *conf.Config) *RaftBackend {
	c := config2.Config{
		NodeID:conf.GlobalConf.RaftConf.NodeID,
		ElectionRTT:10,
		HeartbeatRTT:2,
		CheckQuorum:true,
		SnapshotEntries:5000,
		CompactionOverhead:500,
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