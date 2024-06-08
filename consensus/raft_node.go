package consensus

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"raft_example/pkg"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/pkg/errors"
)

type RaftNode struct {
	id            uint64
	isJoin        bool
	ctx           context.Context
	store         *raft.MemoryStorage
	cfg           *raft.Config
	raft          raft.Node
	membership    *Cluster
	ticker        *time.Ticker
	snapTick      int
	idGenerator   pkg.IDGenerator
	snapdir       string
	snapshotter   *snap.Snapshotter
	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64
	snapCount     uint64

	mu sync.Mutex

	transport  *rafthttp.Transport
	httpServer *http.Server
	enableTLS  bool
	keyFile    string
	certFile   string

	waldir string
	wal    *wal.WAL

	proposeC      chan []byte
	configChangeC chan raftpb.ConfChange
	stopC         chan struct{}
	doneC         chan struct{}

	errorC chan error

	engine Engine
}

func NewRaftNode(raftCfg *RaftNodeConfig) *RaftNode {
	store := raft.NewMemoryStorage()
	id := raftCfg.Id

	if len(raftCfg.SnapDir) == 0 {
		raftCfg.SnapDir = fmt.Sprintf("demo-%d-snap", id)
	}
	if len(raftCfg.WalDir) == 0 {
		raftCfg.WalDir = fmt.Sprintf("demo-%d-wal", id)
	}

	return &RaftNode{
		id:         id,
		isJoin:     raftCfg.IsJoin,
		ctx:        raftCfg.Ctx,
		store:      store,
		membership: NewCluster(raftCfg.Peers),
		cfg: &raft.Config{
			ID:              id,
			ElectionTick:    raftCfg.ElectionTick,
			HeartbeatTick:   raftCfg.HeartbeatTick,
			Storage:         store,
			MaxSizePerMsg:   math.MaxUint16,
			MaxInflightMsgs: 256,
		},
		ticker:   raftCfg.Ticker,
		snapTick: raftCfg.SnapTick,

		snapdir:   raftCfg.SnapDir,
		snapCount: DefaultSnapshotCount,
		waldir:    raftCfg.WalDir,

		proposeC:      make(chan []byte),
		configChangeC: make(chan raftpb.ConfChange),
		stopC:         make(chan struct{}),
		errorC:        make(chan error),
		doneC:         make(chan struct{}),

		engine:      raftCfg.Engine,
		idGenerator: raftCfg.IDGenerator,
	}
}

func (rc *RaftNode) Start() error {
	if !pkg.FileExisted(rc.snapdir) {
		if err := os.MkdirAll(rc.snapdir, 0750); err != nil {
			return errors.Wrap(err, "Failed creating snapshot dir")
		}
	}
	isRestart := wal.Exist(rc.waldir)

	rc.snapshotter = snap.New(rc.snapdir)
	if err := rc.replayWAL(); err != nil {
		return err
	}

	var p []raft.Peer
	for k, v := range rc.membership.members {
		p = append(p, raft.Peer{ID: k, Context: []byte(v)})
	}

	if isRestart || rc.isJoin {
		rc.raft = raft.RestartNode(rc.cfg)
	} else {
		rc.raft = raft.StartNode(rc.cfg, p)
	}

	snap, err := rc.store.Snapshot()
	if err != nil {
		return err
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	if err := rc.httpTransportStart(); err != nil {
		return err
	}
	rc.serveProposeChannels()
	rc.serveRaftHandlerChannels()

	return nil
}

func (rc *RaftNode) Stop() {
	if rc.stopC == nil {
		return
	}
	defer rc.mu.Unlock()
	rc.mu.Lock()
	close(rc.stopC)
	rc.stopC = nil
	rc.raft.Stop()
	rc.wal.Close()
	rc.httpServer.Close()
	rc.transport.Stop()

	close(rc.doneC)
}

func (rc *RaftNode) Propose(data []byte) {
	rc.proposeC <- data
}

func (rc *RaftNode) AddNodes(peers map[uint64]string) {
	for nodeID, url := range peers {

		rc.configChangeC <- raftpb.ConfChange{
			Context: []byte(url),
			Type:    raftpb.ConfChangeAddNode,
			NodeID:  nodeID,
		}
	}
}

func (rc *RaftNode) GetLeader() uint64 {
	return rc.raft.Status().Lead
}

func (rc *RaftNode) GetId() uint64 {
	return rc.id
}

func (rc *RaftNode) IsLeader() bool {
	return rc.GetLeader() == rc.id
}

func (rc *RaftNode) CatchError() <-chan error {
	return rc.errorC
}

func (rc *RaftNode) Done() <-chan struct{} {
	return rc.doneC
}
