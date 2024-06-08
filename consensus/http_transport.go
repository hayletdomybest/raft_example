package consensus

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/pkg/errors"
)

func (rc *RaftNode) httpTransportStart() error {
	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.id),
		ClusterID:   1,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(rc.id))),
		ErrorC:      make(chan error),
	}

	if err := rc.transport.Start(); err != nil {
		return errors.Errorf("Failed transport start (%v)", err)
	}

	u, err := url.Parse(rc.membership.GetURL(rc.id))
	if err != nil {
		return errors.Errorf("Failed parsing URL (%v)", err)
	}
	for id, member := range rc.membership.members {
		rc.transport.AddPeer(types.ID(id), []string{member})
	}

	var ln net.Listener
	if rc.enableTLS {
		cert, err := tls.LoadX509KeyPair(rc.certFile, rc.keyFile)
		if err != nil {
			return errors.Errorf("Failed loading cert (%v)", err)
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
		ln, err = tls.Listen("tcp", u.Host, tlsConfig)
		if err != nil {
			return errors.Errorf("Failed listening (%v)", err)
		}
	} else {
		ln, err = net.Listen("tcp", u.Host)
		if err != nil {
			errors.Errorf("Failed listening (%v)", err)
		}
	}

	rc.httpServer = &http.Server{Handler: rc.transport.Handler()}

	go func() {
		defer rc.httpServer.Close()
		fmt.Printf("node%d is listening on addr %s\n", rc.id, ln.Addr().String())
		err = rc.httpServer.Serve(ln)
		if err != nil && err != http.ErrServerClosed {
			rc.errorC <- errors.Errorf("Http server close (%v)", err)
		}
	}()

	return nil
}

func (n *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return n.raft.Step(ctx, m)
}

func (n *RaftNode) IsIDRemoved(id uint64) bool {
	return !n.membership.HasMember(id)
}

func (n *RaftNode) ReportUnreachable(id uint64) {
	n.raft.ReportUnreachable(id)
}

func (n *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.raft.ReportSnapshot(id, status)
}
