package consensus

import (
	"os"

	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/pkg/errors"
)

func (rc *RaftNode) initWal() error {
	if wal.Exist(rc.waldir) {
		return errors.Errorf("wal is exist")
	}
	if err := os.MkdirAll(rc.waldir, 0750); err != nil {
		return errors.Errorf("cannot create dir for wal (%v)", err)
	}

	w, err := wal.Create(rc.waldir, nil)
	if err != nil {
		return errors.Errorf("create wal error (%v)", err)
	}
	w.Close()

	return nil
}

func (rc *RaftNode) replayWAL() error {
	if !wal.Exist(rc.waldir) {
		if err := rc.initWal(); err != nil {
			return err
		}
	}

	walSnaps, err := wal.ValidSnapshotEntries(rc.waldir)
	if err != nil {
		return err
	}
	snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
	if err != nil && err != snap.ErrNoSnapshot {
		return err
	}
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		rc.engine.ReloadSnapshot(snapshot.Data)
		rc.store.ApplySnapshot(*snapshot)
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		return err
	}

	rc.wal = w

	_, st, ents, err := rc.wal.ReadAll()
	if err != nil {
		return err
	}
	rc.store.SetHardState(st)
	rc.store.Append(ents)

	return nil
}
