package stores

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/modules"
)

// EphemeralSatelliteStore implements worker.SatelliteStore in memory.
type EphemeralSatelliteStore struct {
	mu     sync.Mutex
	config api.SatelliteConfig
}

// Config implements worker.SatelliteStore.
func (s *EphemeralSatelliteStore) Config() api.SatelliteConfig {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// SetConfig implements worker.SatelliteStore.
func (s *EphemeralSatelliteStore) SetConfig(c api.SatelliteConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = c
	return nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *EphemeralSatelliteStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	panic("not implemented")
}

// NewEphemeralSatelliteStore returns a new EphemeralSatelliteStore.
func NewEphemeralSatelliteStore() *EphemeralSatelliteStore {
	return &EphemeralSatelliteStore{}
}

// JSONAutopilotStore implements worker.SatelliteStore in memory, backed by a JSON file.
type JSONSatelliteStore struct {
	*EphemeralSatelliteStore
	dir      string
	lastSave time.Time
}

type jsonSatellitePersistData struct {
	Config api.SatelliteConfig
}

func (s *JSONSatelliteStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p jsonSatellitePersistData
	p.Config = s.config
	js, _ := json.MarshalIndent(p, "", "  ")

	// atomic save
	dst := filepath.Join(s.dir, "satellite.json")
	f, err := os.OpenFile(dst+"_tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(js); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	} else if err := os.Rename(dst+"_tmp", dst); err != nil {
		return err
	}
	return nil
}

func (s *JSONSatelliteStore) load() error {
	var p jsonSatellitePersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "satellite.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	s.config = p.Config
	return nil
}

// SetConfig implements worker.SatelliteStore.
func (s *JSONSatelliteStore) SetConfig(c api.SatelliteConfig) error {
	s.EphemeralSatelliteStore.SetConfig(c)
	return s.save()
}

// NewJSONSatelliteStore returns a new JSONSatelliteStore.
func NewJSONSatelliteStore(dir string) (*JSONSatelliteStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	s := &JSONSatelliteStore{
		EphemeralSatelliteStore: NewEphemeralSatelliteStore(),
		dir:                     dir,
		lastSave:                time.Now(),
	}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
