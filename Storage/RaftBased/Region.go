package RaftBased

import (
	"Puzzle/conf"
	pb "Puzzle/idl"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/golang/protobuf/proto"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

type RegionStateMachine struct {
	clusterID uint64
	nodeID uint64
	db *redis.Client
	SnapshotPath string
	redisDockerID string
}

func generateID(nodeID, clusterID uint64) uint64 {
	// todo
	return clusterID * 100 + nodeID
}

func (rsm *RegionStateMachine)newRedisInstance(RDBPath string) (*conf.RedisConfig, error){
	//todo
	fmt.Println("starting a new redis instance")
	cmd := exec.Command("docker",
		"run",
		"-d",
		"--rm",
		"--name", "redis_node"+strconv.FormatUint(generateID(rsm.nodeID, rsm.clusterID), 10),
		"-p", strconv.FormatUint(10000+generateID(rsm.nodeID, rsm.clusterID), 10)+":"+"6379",
		"-v", RDBPath+":"+"/data/",
		"redis",
		)
	log.Println(cmd.String())
	out, err := cmd.Output()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	rsm.redisDockerID = string(out)
	return &conf.RedisConfig{
		Host:"127.0.0.1",
		Port:strconv.FormatUint(10000+generateID(rsm.nodeID, rsm.clusterID), 10),
		Password:"",
		RDBLocation:RDBPath,
	}, nil
}

func (rsm *RegionStateMachine) closeRedisInstance(id string) error {
	return exec.Command("docker", "stop", id).Run()
}

func NewRegionStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	rsm := &RegionStateMachine{
		clusterID:clusterID,
		nodeID:nodeID,
		SnapshotPath:conf.GlobalConf.RedisConf.RDBLocation + strconv.FormatUint(clusterID, 10) + strconv.FormatUint(nodeID, 10),
	}

	c, err := rsm.newRedisInstance(conf.GlobalConf.RedisConf.RDBLocation)
	if err != nil {
		panic(err)
	}
	db := redis.NewClient(&redis.Options{
		Addr:c.Host+":"+c.Port,
		Password:c.Password,
		DB:0,
	})
	rsm.db = db
	return rsm
}

func (rsm *RegionStateMachine) Lookup(query interface{}) (interface{}, error) {
	if query == nil {
		return nil, errors.New("query is nil")
	}
	if c, ok := query.([]string); ok{
		switch c[0] {
		case "GET":
			return rsm.db.Get(c[1]).Result()
		case "SET":
			val, _ := strconv.ParseInt(c[3], 10, 64)
			return rsm.db.Set(c[1], c[2], time.Duration(val) *time.Second).Result()
		case "HSET":
			return rsm.db.HSet(c[1], c[2], c[3], c[4]).Result()
		case "HGETALL":
			return rsm.db.HGetAll(c[1]).Result()
		}
	}
	return nil, errors.New("query should be a string slice type")
}

func (rsm *RegionStateMachine) Update(data []byte) (sm.Result, error) {
	proposal := &pb.RaftProposal{}
	if err := proto.Unmarshal(data, proposal); err != nil {
		return sm.Result{}, err
	}
	args := make([]interface{}, len(proposal.Cmds))
	for i, val := range proposal.Cmds {
		args[i] = *(*string)(unsafe.Pointer(&val))
	}
	if res, err := rsm.db.Do(args...).Result(); err != nil {
		return sm.Result{}, err
	} else {
		log.Println(res)
		return sm.Result{}, nil
	}
}

func (rsm *RegionStateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	// let redis generate the snapshot
	// todo: use bgsave? or implement the save cmd
	res, err := rsm.db.Save().Result()
	if err != nil {
		return err
	}
	log.Println("starting creating snapshot")
	log.Println(res)
	f, err := os.Open(rsm.SnapshotPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(w, f)

	return err
}

// recover should only happen in two conditions:
// 1. synchronizing from a remote node
// 2. reboot
// if the raft group is down, then all the snapshots should be discarded
func (rsm *RegionStateMachine)RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error  {

	// new redis load the rdb file
	// during the loading, no saving snapshot will happen
	// so it's safe to use the original one directly
	oldID := rsm.redisDockerID
	c, err := rsm.newRedisInstance(rsm.SnapshotPath)
	if err != nil {
		return err
	}

	db := redis.NewClient(&redis.Options{
		Addr:c.Host+":"+c.Port,
		Password:c.Password,
		DB:0,
	})
	ptr := unsafe.Pointer(rsm.db)
	old := (*redis.Client)(atomic.SwapPointer(&ptr, unsafe.Pointer(db)))
	if old != nil {
		old.Close()
		log.Println("closing the old redis instance")
		err := rsm.closeRedisInstance(oldID)
		if err != nil {
			log.Println("close redis instance failed")
		}
	}
	return nil
}

func (rsm *RegionStateMachine) Close() error {
	rsm.db.Close()
	return rsm.closeRedisInstance(rsm.redisDockerID)
}

func (rsm *RegionStateMachine) GetHash() (uint64, error) {
	// todo
	h := sha256.New()
	var buf []byte
	binary.LittleEndian.PutUint64(buf, rsm.clusterID)
	h.Write(buf)
	binary.LittleEndian.PutUint64(buf, rsm.nodeID)
	h.Write(buf)
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8]), nil
}