package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"raft_example/consensus"
	"raft_example/pkg"
	"strconv"
	"sync"
	"time"
)

func createNode(id uint64, peers map[uint64]string, isJoin bool, baseDir string) (engine *MyEngine, node *consensus.RaftNode) {
	engine = NewMyEngine()
	engine.SetId(id)

	node = consensus.NewRaftNode(&consensus.RaftNodeConfig{
		Id:            id,
		IsJoin:        isJoin,
		Ctx:           context.TODO(),
		ElectionTick:  consensus.DefaultElectionTick,
		HeartbeatTick: consensus.DefaultHeartBeatTick,
		Ticker:        time.NewTicker(100 * time.Millisecond),
		SnapTick:      100,
		Peers:         peers,
		SnapDir:       filepath.Join(baseDir, fmt.Sprintf("sanp-%d", id)),
		WalDir:        filepath.Join(baseDir, fmt.Sprintf("wal-%d", id)),
		Engine:        engine,

		IDGenerator: pkg.NewSnowFlake(),
	})

	return engine, node
}

func basicTest(ctx context.Context, askJoin <-chan map[uint64]string) {
	peers := map[uint64]string{
		1: "http://localhost:7701",
		2: "http://localhost:7702",
		3: "http://localhost:7703",
	}

	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "data")

	var nodes map[uint64]*consensus.RaftNode = make(map[uint64]*consensus.RaftNode)
	var engines map[uint64]*MyEngine = make(map[uint64]*MyEngine)

	var wgStop sync.WaitGroup
	var wgLeadership sync.WaitGroup

	var nodeCount = 3
	for i := 0; i < nodeCount; i++ {
		wgStop.Add(1)
		wgLeadership.Add(1)

		var id uint64 = uint64(i + 1)
		engines[id], nodes[id] = createNode(id, peers, false, dir)

		node := nodes[id]

		defer node.Stop()
		if err := node.Start(); err != nil {
			panic(err)
		}

		go func() {
			defer wgStop.Done()
			defer node.Stop()

			select {
			case err := <-node.CatchError():
				fmt.Printf("node %d catch error %v\n", node.GetId(), err)
			case <-node.Done():
				fmt.Printf("node %d done\n", node.GetId())
			}
		}()

		go func() {
			defer wgLeadership.Done()

			var leader uint64
			for leader == 0 {
				leader = node.GetLeader()
			}
			fmt.Printf("node %d finish election leader is %d\n", id, leader)
		}()
	}

	wgLeadership.Wait()

	var sender *consensus.RaftNode
	for _, node := range nodes {
		if !node.IsLeader() {
			sender = node
			break
		}
	}

	go func() {
		for i := 1; i <= 10; i++ {
			data := DataModel{Message: strconv.Itoa(i)}
			bz, _ := json.Marshal(&data)
			sender.Propose(bz)
			time.Sleep(300 * time.Microsecond)
		}

		fmt.Printf("All data has been sent\n")
		time.Sleep(5 * time.Second)

		for _, engine := range engines {
			allData := engine.GetAll(true)
			for _, data := range allData {
				fmt.Printf("engineID:%d message:%s\n", engine.id, data.Message)
			}
		}
	}()

	go func() {
		for {
			select {
			case m := <-askJoin:
				for id := range m {
					fmt.Printf("join node %d\n", id)
				}
				nodes[1].AddNodes(m)
			case <-ctx.Done():
				return
			}

		}
	}()

	<-ctx.Done()
}

func joinTest(ctx context.Context) {
	askJoin := make(chan map[uint64]string)
	go basicTest(ctx, askJoin)

	time.Sleep(2 * time.Second)

	fmt.Println("start join")

	peers := map[uint64]string{
		1: "http://localhost:7701",
		2: "http://localhost:7702",
		3: "http://localhost:7703",
		4: "http://localhost:7704",
	}

	dir, _ := os.Getwd()
	dir = filepath.Join(dir, "data")

	engine, node := createNode(4, peers, true, dir)

	defer func() {
		fmt.Println("node 4 done")
		node.Stop()
	}()
	if err := node.Start(); err != nil {
		panic(err)
	}

	askJoin <- map[uint64]string{4: "http://localhost:7704"}

	fmt.Println("wait for catch up 2 sec")
	time.Sleep(2 * time.Second)

	data := engine.GetAll(true)
	for _, d := range data {
		fmt.Printf("join engine: data id:%d message:%s\n", d.Id, d.Message)
	}

	<-ctx.Done()
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go joinTest(ctx)
	time.Sleep(10 * time.Second)
	cancel()
	time.Sleep(2 * time.Second)
	fmt.Println("main has done")
}
