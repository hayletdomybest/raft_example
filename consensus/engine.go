package consensus

type Engine interface {
	Process(data []byte) error

	GetSnapshot() ([]byte, error)
	ReloadSnapshot([]byte) error
}
