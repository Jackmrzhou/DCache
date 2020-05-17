package RaftBased

import (
	. "Puzzle/Logger"
	"Puzzle/conf"
	pb "Puzzle/idl"
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	config2 "github.com/lni/dragonboat/v3/config"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

var mutex = sync.Mutex{}
var jobDone = int32(0)
var staleKeys = sync.Map{}


type RaftBackend struct {
	nh *dragonboat.NodeHost
	taskRunner *TimedTaskRunner
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

//Deprecated
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

func (rb *RaftBackend) HMSet(key string, fields map[string]interface{}) (e error) {
	defer func() {
		if e != nil {
			staleKeys.Store(key[:len(key)-1], true)
		}
	}()
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	cs := rb.nh.GetNoOPSession(ClusterID1)
	buf := new(bytes.Buffer)
	gob.Register(map[string]interface{}{})
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
	res, err := rb.nh.SyncPropose(ctx, cs, data)
	if err != nil {
		// set fields failed
		return err
	}

	rowKey := key[:len(key)-1]
	if res.Value == OOM_ERROR_CODE {
		// memory eviction should be only entered once
		atomic.StoreInt32(&jobDone, 0)
		mutex.Lock()
		if jobDone == 1 {
			Logger.Infof("memory eviction is done by other goroutine.")
			return errors.New("out of memory, please try again later")
			// other goroutine has done the job
		}
		jobDone = 1
		defer mutex.Unlock()

		// out of memory
		Logger.Infof("achieve memory limitation, start evicting keys....")
		var keysToDel []string
		keysToDel = append(keysToDel, rowKey)
		// get row keys to be deleted
		ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
		res, err := rb.nh.SyncRead(ctx, ClusterID1, []string{"ZCARD", RowSet})
		if err != nil {
			return err
		}
		end := res.(int64) / 3
		Logger.Infof("total %d keys to be evicted", end+1)

		for start := int64(0); start < end; start += 10000 {
			ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
			count := int64(10000)
			if start + count > end {
				count = end - start
			}
			res, err = rb.nh.SyncRead(ctx, ClusterID1, []interface{}{"ZRANGE", RowSet, int64(0), count})
			if err != nil {
				return err
			}
			// delete associated hash table from redis
			err := rb.DeleteRowKeys(res.([]string))
			if err != nil {
				Logger.Errorf(err.Error())
				return err
			}
			// delete from sorted set
			ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
			proposal = &pb.RaftProposal{Cmds: [][]byte{
				[]byte("ZREMRANGEBYRANK"),
				[]byte(RowSet),
				[]byte(strconv.FormatInt(start, 10)),
				[]byte(strconv.FormatInt(count, 10)),
			}}
			data, _ = proto.Marshal(proposal)
			_, err = rb.nh.SyncPropose(ctx, cs, data)
			if err != nil {
				return err
			}
		}

		Logger.Infof("evicting keys done!")
		return errors.New("out of memory, please try again later")
	}

	// update rank
	// failure is ignored here as it's insignificant
	ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
	proposal = &pb.RaftProposal{Cmds: [][]byte{
		[]byte("ZINCRBY"),
		[]byte(RowSet),
		[]byte("1"),
		[]byte(rowKey),
	}}
	data, _ = proto.Marshal(proposal)
	_, err = rb.nh.SyncPropose(ctx, cs, data)

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

func (rb *RaftBackend) DeleteRowKeys(keys []string) error {
	cs := rb.nh.GetNoOPSession(ClusterID1)
	shortCfs := []string{"g", "h", "i", "e", "f", "s", "t", "m", "l"}
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	cmd := make([][]byte, len(keys) * len(shortCfs) + 1)
	cmd[0] = []byte("DEL")
	for i, val :=range keys {
		for j, cf := range shortCfs {
			cmd[1+i*len(shortCfs) + j] = []byte(val + cf)
		}
	}
	proposal := &pb.RaftProposal{Cmds: cmd}
	data, _ := proto.Marshal(proposal)
	_, err := rb.nh.SyncPropose(ctx, cs, data)
	if err != nil {
		return err
	}
	return nil
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
		SnapshotEntries:50000,
		CompactionOverhead:1000,
	}
	initialMembers := config.RaftConf.InitialMembers
	nodeID := config.RaftConf.NodeID
	dataDir := config.RaftConf.DataDir
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

	rb := &RaftBackend{
		nh:nh,
		taskRunner:&TimedTaskRunner{},
	}

	rb.taskRunner.SubmitTask(1*time.Second, func() {
		var keys []string
		staleKeys.Range(func(key, value interface{}) bool {
			keys = append(keys, key.(string))
			return true
		})
		if len(keys) != 0 {
			Logger.Infof("starting removing stale keys...")
			err := rb.DeleteRowKeys(keys)
			if err != nil {
				Logger.Warningf("Removing stale keys failed, err: %v", err)
				return
			}
			for _, k := range keys {
				staleKeys.Delete(k)
			}
			Logger.Infof("removing stale keys done.")
		}
	})

	return rb
}