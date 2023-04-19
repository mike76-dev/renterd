package satellite

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"

	"go.uber.org/zap"
)

// busClient is the interface for renterd/bus.
type busClient interface {
	AddContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, spk types.PublicKey) (api.ContractMetadata, error)
	AddRenewedContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, spk types.PublicKey) (api.ContractMetadata, error)
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	GougingParams(ctx context.Context) (api.GougingParams, error)
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
}

// Satellite is the interface between the renting software and the
// Sia Satellite node.
type Satellite struct {
	bus    busClient
	store  jsonStore
	logger *zap.SugaredLogger
}

// NewSatellite returns a new Satellite handler.
func NewSatellite(bc busClient, dir string, l *zap.Logger, satAddr string, satPassword string) (http.Handler, error) {
	satelliteDir := filepath.Join(dir, "satellite")
	if err := os.MkdirAll(satelliteDir, 0700); err != nil {
		return nil, err
	}
	ss, err := newJSONStore(satelliteDir)
	if err != nil {
		return nil, err
	}

	s, err := New(bc, *ss, l)
	if err != nil {
		return nil, err
	}

	// Initialize the client.
	StaticSatellite = NewClient(satAddr, satPassword)

	return s.Handler(), nil
}

// New returns a new Satellite.
func New(bc busClient, ss jsonStore, l *zap.Logger) (*Satellite, error) {
	s := &Satellite{
		bus:    bc,
		store:  ss,
		logger: l.Sugar().Named("satellite"),
	}
	_, span := tracing.Tracer.Start(context.Background(), "satellite.New")
	defer span.End()

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

// Handler returns an HTTP handler that serves the satellite API.
func (s *Satellite) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("satellite", map[string]jape.Handler{
		"GET    /request":      s.requestContractsHandler,
		"POST   /form":         s.formContractsHandler,
		"POST   /renew":        s.renewContractsHandler,
		"POST   /update":       s.updateRevisionHandler,
		"GET    /config":       s.configHandlerGET,
		"PUT    /config":       s.configHandlerPUT,
		"PUT    /contract":     s.contractHandlerPUT,
		"DELETE /contract/:id": s.contractHandlerDELETE,
		"GET    /contract/:id": s.contractHandlerGET,
		"GET    /contracts":    s.contractsHandlerGET,
		"DELETE /contracts":    s.contractsHandlerDELETE,
	}))
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
