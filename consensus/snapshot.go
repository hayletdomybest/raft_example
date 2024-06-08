package consensus

import (
	"log"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal/walpb"
)

func (rc *RaftNode) applySoftSnap(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("apply soft snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished apply soft snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("fail: apply snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

func (rc *RaftNode) applyHardSnap(snap raftpb.Snapshot) error {
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftNode) triggerSnap() {
	if rc.appliedIndex == rc.snapshotIndex {
		return
	}

	data, err := rc.engine.GetSnapshot()
	if err != nil {
		rc.errorC <- err
		return
	}

	snap, err := rc.store.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		rc.errorC <- err
		return
	}

	if err := rc.applyHardSnap(snap); err != nil {
		rc.errorC <- err
		return
	}

	var compactIndex uint64
	if rc.appliedIndex > SnapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - SnapshotCatchUpEntriesN
	}
	if err := rc.store.Compact(compactIndex); err != nil {
		if err != raft.ErrCompacted {
			rc.errorC <- err
			return
		}
	}
	rc.snapshotIndex = rc.appliedIndex
}

// When a `raftpb.EntryConfChange` appears after generating a snapshot,
// the confState included in the snapshot becomes outdated. Therefore,
// we need to update the confState before sending the snapshot to the follower.

// Note: `raftpb.MsgSnap` messages are only sent by the leader node.
// Follower nodes do not send such messages. Therefore, it is safe to
// assume in the application logic that these messages come from the leader
// node and update the confState in the snapshot accordingly.
func (rc *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}
