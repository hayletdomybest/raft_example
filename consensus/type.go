package consensus

import (
	"context"
	"raft_example/pkg"
	"time"
)

type RaftNodeConfig struct {
	Id            uint64
	IsJoin        bool
	Ctx           context.Context
	ElectionTick  int
	HeartbeatTick int
	Ticker        *time.Ticker
	SnapTick      int
	Peers         map[uint64]string
	SnapDir       string
	WalDir        string
	Engine        Engine
	IDGenerator   pkg.IDGenerator
}

type RaftNodeState uint16

const (
	New RaftNodeState = iota
	Running
	Done
)
