package satellite

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"

	"go.uber.org/zap"

	"golang.org/x/crypto/blake2b"
)

// autopilotClient is the interface for renterd/autopilot.
type autopilotClient interface {
	Config() (cfg api.AutopilotConfig, err error)
}

// busClient is the interface for renterd/bus.
type busClient interface {
	AddContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, spk types.PublicKey) (api.ContractMetadata, error)
	AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
	AddRenewedContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, spk types.PublicKey) (api.ContractMetadata, error)
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context) ([]api.ContractMetadata, error)
	ContractSetContracts(ctx context.Context, set string) (contracts []api.ContractMetadata, err error)
	DeleteObject(ctx context.Context, bucket, path string, batch bool) error
	GougingParams(ctx context.Context) (api.GougingParams, error)
	Object(ctx context.Context, path string, options ...api.ObjectsOption) (api.Object, []api.ObjectMetadata, error)
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
}

// Satellite is the interface between the renting software and the
// Sia Satellite node.
type Satellite struct {
	ap         autopilotClient
	bus        busClient
	store      jsonStore
	logger     *zap.SugaredLogger
	renterKey  types.PrivateKey
	accountKey types.PrivateKey
}

// deriveRenterKey is used to derive a sub-masterkey from the worker's
// masterKey to use for forming contracts with the hosts.
func deriveRenterKey(key [32]byte) types.PrivateKey {
	seed := blake2b.Sum256(append(key[:], []byte("renterkey")...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// deriveAccountKey is used to derive a sub-masterkey from the worker's
// masterKey to use for accessing the ephemeral accounts at the hosts.
func deriveAccountKey(key [32]byte) types.PrivateKey {
	seed := blake2b.Sum256(append(key[:], []byte("accountkey")...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// NewSatellite returns a new Satellite handler.
func NewSatellite(ac autopilotClient, bc busClient, dir string, seed types.PrivateKey, l *zap.Logger, satAddr string, satPassword string) (http.Handler, error) {
	satelliteDir := filepath.Join(dir, "satellite")
	if err := os.MkdirAll(satelliteDir, 0700); err != nil {
		return nil, err
	}
	ss, err := newJSONStore(satelliteDir)
	if err != nil {
		return nil, err
	}

	s, err := New(ac, bc, *ss, seed, l)
	if err != nil {
		return nil, err
	}

	// Initialize the client.
	StaticSatellite = NewClient(satAddr, satPassword)

	return s.Handler(), nil
}

// New returns a new Satellite.
func New(ac autopilotClient, bc busClient, ss jsonStore, seed types.PrivateKey, l *zap.Logger) (*Satellite, error) {
	s := &Satellite{
		ap:         ac,
		bus:        bc,
		store:      ss,
		renterKey:  deriveRenterKey(blake2b.Sum256(append([]byte("worker"), seed...))),
		accountKey: deriveAccountKey(blake2b.Sum256(append([]byte("worker"), seed...))),
		logger:     l.Sugar().Named("satellite"),
	}

	// Save the satellite config.
	if cfg.Enabled {
		err := s.store.setConfig(cfg)
		if err != nil {
			s.logger.Errorw(fmt.Sprintf("failed to save satellite config: %v", err))
		}
	}

	return s, nil
}

// configHandlerGET handles the GET /config requests.
func (s *Satellite) configHandlerGET(jc jape.Context) {
	jc.Encode(s.store.getConfig())
}

// configHandlerPUT handles the PUT /config requests.
func (s *Satellite) configHandlerPUT(jc jape.Context) {
	var sc Config
	if jc.Decode(&sc) != nil {
		return
	}
	if jc.Check("failed to set config", s.store.setConfig(sc)) != nil {
		return
	}
	if sc.Enabled {
		s.requestContractsHandler(jc)
	}
}

// contractHandlerPUT handles the PUT /contract request.
func (s *Satellite) contractHandlerPUT(jc jape.Context) {
	var car ContractAddRequest
	if jc.Decode(&car) != nil {
		return
	}
	jc.Check("failed to add contract to the store", s.store.addContract(car.FCID, car.PK))
}

// contractHandlerDELETE handles the DELETE /contract request.
func (s *Satellite) contractHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("failed to delete contract from the store", s.store.deleteContract(id))
}

// contractHandlerGET handles the GET /contract requests.
func (s *Satellite) contractHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	pk, exists := s.store.satellite(id)
	if !exists {
		pk = types.PublicKey{}
	}
	jc.Encode(pk)
}

// contractsHandlerGET handles the GET /contracts requests.
func (s *Satellite) contractsHandlerGET(jc jape.Context) {
	c := ContractsAllResponse{
		Contracts: s.store.getContracts(),
	}
	jc.Encode(c)
}

// contractsHandlerDELETE handles the DELETE /contracts requests.
func (s *Satellite) contractsHandlerDELETE(jc jape.Context) {
	s.store.deleteAll()
}

// satelliteHandlerPUT handles the PUT /satellite request.
func (s *Satellite) satelliteHandlerPUT(jc jape.Context) {
	var si SatelliteInfo
	if jc.Decode(&si) != nil {
		return
	}
	jc.Check("failed to add satellite to the store", s.store.addSatellite(si))
}

// satelliteHandlerGET handles the GET /satellite requests.
func (s *Satellite) satelliteHandlerGET(jc jape.Context) {
	var pk types.PublicKey
	if jc.DecodeParam("id", &pk) != nil {
		return
	}
	satellite, exists := s.store.getSatellite(pk)
	if !exists {
		jc.Check("ERROR", errors.New("unknown satellite"))
		return
	}
	jc.Encode(satellite)
}

// satellitesHandlerGET handles the GET /satellites requests.
func (s *Satellite) satellitesHandlerGET(jc jape.Context) {
	sar := SatellitesAllResponse{
		Satellites: s.store.getSatellites(),
	}
	jc.Encode(sar)
}

// Handler returns an HTTP handler that serves the satellite API.
func (s *Satellite) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /request":       s.requestContractsHandler,
		"POST   /form":          s.formContractsHandler,
		"POST   /renew":         s.renewContractsHandler,
		"POST   /update":        s.updateRevisionHandler,
		"GET    /config":        s.configHandlerGET,
		"PUT    /config":        s.configHandlerPUT,
		"PUT    /contract":      s.contractHandlerPUT,
		"DELETE /contract/:id":  s.contractHandlerDELETE,
		"GET    /contract/:id":  s.contractHandlerGET,
		"GET    /contracts":     s.contractsHandlerGET,
		"DELETE /contracts":     s.contractsHandlerDELETE,
		"PUT    /satellite":     s.satelliteHandlerPUT,
		"GET    /satellite/:id": s.satelliteHandlerGET,
		"GET    /satellites":    s.satellitesHandlerGET,
		"POST   /rspv2/form":    s.formContractHandler,
		"POST   /rspv2/renew":   s.renewContractHandler,
		"GET    /settings":      s.settingsHandlerGET,
		"POST   /settings":      s.settingsHandlerPOST,
		"POST   /metadata":      s.saveMetadataHandler,
		"GET    /metadata/:set": s.requestMetadataHandler,
		"POST   /slab":          s.updateSlabHandler,
	})
}

// parseEnvVar checks if the env variable is set and reads it.
func parseEnvVar(s string, v interface{}) {
	if env, ok := os.LookupEnv(s); ok {
		if _, err := fmt.Sscan(env, v); err != nil {
			log.Fatalf("failed to parse %s: %v", s, err)
		}
		fmt.Printf("Using %s environment variable\n", s)
	}
}

var StaticSatellite *Client
var cfg Config

// init initializes the package.
func init() {
	parseEnvVar("RENTERD_SATELLITE_ENABLED", &cfg.Enabled)
	parseEnvVar("RENTERD_SATELLITE_ADDR", &cfg.Address)
	parseEnvVar("RENTERD_SATELLITE_KEY", &cfg.PublicKey)
	parseEnvVar("RENTERD_SATELLITE_SEED", &cfg.RenterSeed)
}
