package consensus

import (
	"errors"
	"fmt"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

func (rc *RaftNode) serveProposeChannels() {
	go func() {
		defer close(rc.proposeC)
		defer close(rc.configChangeC)

		for {
			select {
			case <-rc.stopC:
				return
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					rc.raft.Propose(rc.ctx, prop)
				}
			case cc, ok := <-rc.configChangeC:
				if !ok {
					rc.configChangeC = nil
				} else {
					id, err := rc.idGenerator.Generate()
					if err != nil {
						rc.errorC <- err
						return
					}
					cc.ID = id
					rc.raft.ProposeConfChange(rc.ctx, cc)
				}
			}
		}
	}()
}

func (rc *RaftNode) serveRaftHandlerChannels() {
	go func() {
		snapCount := 0
		defer rc.ticker.Stop()
		for {
			select {
			case <-rc.stopC:
				return
			case <-rc.ticker.C:
				rc.raft.Tick()
				snapCount++
				if snapCount >= rc.snapTick {
					rc.mu.Lock()
					rc.triggerSnap()
					snapCount = 0
					rc.mu.Unlock()
				}
			case rd := <-rc.raft.Ready():
				rc.mu.Lock()
				if !raft.IsEmptySnap(rd.Snapshot) {
					fmt.Printf("node %d has snapshot at index %d term %d\n", rc.id, rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Term)
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					rc.applyHardSnap(rd.Snapshot)
				}
				rc.wal.Save(rd.HardState, rd.Entries)
				if !raft.IsEmptySnap(rd.Snapshot) {
					rc.engine.ReloadSnapshot(rd.Snapshot.Data)
					rc.store.ApplySnapshot(rd.Snapshot)
					rc.applySoftSnap(rd.Snapshot)
				}
				rc.store.Append(rd.Entries)
				rc.transport.Send(rc.processMessages(rd.Messages))
				rc.publishEntries(rd.CommittedEntries)
				rc.raft.Advance()
				rc.mu.Unlock()
			}
		}
	}()
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *RaftNode) publishEntries(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}

	for i, ent := range ents {
		if ent.Index <= rc.appliedIndex {
			fmt.Printf("node %d skip entry index %d because node has applied index %d\n", rc.id, ent.Index, rc.appliedIndex)
			continue
		}

		switch ent.Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				break
			}
			rc.engine.Process(ent.Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)

			rc.confState = *rc.raft.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				fmt.Printf("node %d applied ConfChangeAddNode\n", rc.id)
				if len(cc.Context) > 0 {
					rc.membership.AddMember(cc.NodeID, string(cc.Context))
					fmt.Printf("node%d add node%d addr %s\n", rc.id, cc.NodeID, string(cc.Context))
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					rc.errorC <- errors.New("node has been removed from the cluster! shutting down")
					rc.Stop()
					return
				}
				if rc.membership.HasMember(cc.NodeID) {
					rc.membership.RemoveMember(cc.NodeID)
					rc.transport.RemovePeer(types.ID(cc.NodeID))
				}
			}
		}
	}

	select {
	case <-rc.stopC:
		return
	default:
		rc.appliedIndex = ents[len(ents)-1].Index
	}
}
