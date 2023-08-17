package satellite

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/siad/modules"
)

// ephemeralStore implements a satellite store in memory.
type ephemeralStore struct {
	mu         sync.Mutex
	config     Config
	contracts  map[types.FileContractID]types.PublicKey
	satellites map[types.PublicKey]SatelliteInfo
}

// config returns the satellite config.
func (s *ephemeralStore) getConfig() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// setConfig updates the satellite config.
func (s *ephemeralStore) setConfig(c Config) error {
	s.mu.Lock()
	s.config = c
	s.mu.Unlock()
	pk := c.PublicKey
	if (pk != types.PublicKey{}) {
		return s.addSatellite(SatelliteInfo{
			Address:    c.Address,
			PublicKey:  c.PublicKey,
			RenterSeed: c.RenterSeed,
		})
	}
	return nil
}

// contracts returns the map of the satellite contracts.
func (s *ephemeralStore) getContracts() map[types.FileContractID]types.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contracts
}

// satellite returns the satellite public key of the given contract.
func (s *ephemeralStore) satellite(fcid types.FileContractID) (types.PublicKey, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pk, exists := s.contracts[fcid]
	return pk, exists
}

// addContract adds a new contract to the map.
func (s *ephemeralStore) addContract(fcid types.FileContractID, pk types.PublicKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts[fcid] = pk
	return nil
}

// deleteContract deletes a contract from the map.
func (s *ephemeralStore) deleteContract(fcid types.FileContractID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.contracts[fcid]; exists {
		delete(s.contracts, fcid)
	}
	return nil
}

// deleteAll clears the contracts map.
func (s *ephemeralStore) deleteAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.contracts = make(map[types.FileContractID]types.PublicKey)
	return nil
}

// satellites returns the map of the satellites.
func (s *ephemeralStore) getSatellites() map[types.PublicKey]SatelliteInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.satellites
}

// getSatellite returns the information about a specific satellite.
func (s *ephemeralStore) getSatellite(pk types.PublicKey) (SatelliteInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	si, exists := s.satellites[pk]
	return si, exists
}

// addSatellite adds a new satellite to the map.
func (s *ephemeralStore) addSatellite(si SatelliteInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	pk := si.PublicKey
	s.satellites[pk] = si
	return nil
}

// ProcessConsensusChange implements chain.Subscriber.
func (s *ephemeralStore) ProcessConsensusChange(cc modules.ConsensusChange) {
	panic("not implemented")
}

// newEphemeralStore returns a new EphemeralStore.
func newEphemeralStore() *ephemeralStore {
	return &ephemeralStore{
		contracts:  make(map[types.FileContractID]types.PublicKey),
		satellites: make(map[types.PublicKey]SatelliteInfo),
	}
}

// jsonStore implements a satellite store in memory, backed by a JSON file.
type jsonStore struct {
	*ephemeralStore
	dir      string
	lastSave time.Time
}

type jsonPersistData struct {
	Config     Config                                   `json:"config"`
	Contracts  map[types.FileContractID]types.PublicKey `json:"contracts"`
	Satellites map[types.PublicKey]SatelliteInfo        `json:"satellites"`
}

func (s *jsonStore) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var p jsonPersistData
	p.Config = s.config
	p.Contracts = s.contracts
	p.Satellites = s.satellites
	js, _ := json.MarshalIndent(p, "", "  ")

	// Atomic save.
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

func (s *jsonStore) load() error {
	var p jsonPersistData
	if js, err := os.ReadFile(filepath.Join(s.dir, "satellite.json")); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	} else if err := json.Unmarshal(js, &p); err != nil {
		return err
	}
	s.config = p.Config
	s.contracts = p.Contracts
	s.satellites = p.Satellites
	return nil
}

// setConfig updates the satellite config.
func (s *jsonStore) setConfig(c Config) error {
	s.ephemeralStore.setConfig(c)
	return s.save()
}

// addContract adds a new contract to the map.
func (s *jsonStore) addContract(fcid types.FileContractID, pk types.PublicKey) error {
	s.ephemeralStore.addContract(fcid, pk)
	return s.save()
}

// deleteContract deletes a contract from the map.
func (s *jsonStore) deleteContract(fcid types.FileContractID) error {
	s.ephemeralStore.deleteContract(fcid)
	return s.save()
}

// deleteAll clears the contracts map.
func (s *jsonStore) deleteAll() error {
	s.ephemeralStore.deleteAll()
	return s.save()
}

// addSatellite adds a new satellite to the map.
func (s *jsonStore) addSatellite(si SatelliteInfo) error {
	s.ephemeralStore.addSatellite(si)
	return s.save()
}

// newJSONStore returns a new jsonStore.
func newJSONStore(dir string) (*jsonStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	s := &jsonStore{
		ephemeralStore: newEphemeralStore(),
		dir:            dir,
		lastSave:       time.Now(),
	}
	err := s.load()
	if err != nil {
		return nil, err
	}
	return s, nil
}
