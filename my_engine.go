package main

import (
	"encoding/json"
	"fmt"
	"raft_example/consensus"
	"slices"
	"sync"
)

type DataModel struct {
	Id      uint64
	Message string
}

type DataModelSnapshot struct {
	State     map[uint64]DataModel
	AutoIncId uint64
}

type MyEngine struct {
	id uint64
	mu sync.Mutex

	state     map[uint64]DataModel
	autoIncId uint64
}

var _ consensus.Engine = (*MyEngine)(nil)

func NewMyEngine() *MyEngine {
	return &MyEngine{
		state: make(map[uint64]DataModel),
	}
}

func (engine *MyEngine) Append(data ...DataModel) {
	engine.append(true, data...)
}

func (engine *MyEngine) append(lock bool, data ...DataModel) {
	if lock {
		engine.mu.Lock()
		defer engine.mu.Unlock()
	}

	for _, d := range data {
		d.Id = engine.autoIncId
		engine.state[d.Id] = d
		engine.autoIncId++
		fmt.Printf("engine%d process data id:%d message:%s\n", engine.id, d.Id, d.Message)
	}
}

func (engine *MyEngine) GetById(id uint64) (DataModel, bool) {
	data, existed := engine.state[id]

	return data, existed
}

func (engine *MyEngine) GetLastId() uint64 {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	if engine.autoIncId == 0 {
		return engine.autoIncId
	}
	return engine.autoIncId - 1
}

func (engine *MyEngine) GetAll(sort bool) []DataModel {
	var res []DataModel

	for _, v := range engine.state {
		res = append(res, v)
	}
	if sort {
		slices.SortFunc(res, func(a, b DataModel) int {
			return int(a.Id) - int(b.Id)
		})
	}

	return res
}

func (engine *MyEngine) Process(data []byte) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	var model DataModel
	if err := json.Unmarshal(data, &model); err != nil {
		return err
	}
	engine.append(false, model)
	return nil
}

func (engine *MyEngine) GetSnapshot() ([]byte, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	return json.Marshal(DataModelSnapshot{
		State:     engine.state,
		AutoIncId: engine.autoIncId,
	})
}

func (engine *MyEngine) ReloadSnapshot(data []byte) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	var model DataModelSnapshot

	if err := json.Unmarshal(data, &model); err != nil {
		return err
	}

	engine.state = model.State
	engine.autoIncId = model.AutoIncId

	return nil
}

func (engine *MyEngine) SetId(id uint64) {
	engine.id = id
}
