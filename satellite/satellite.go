package satellite

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error)
	AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error
	AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
	AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error)
	Bucket(ctx context.Context, bucketName string) (resp api.Bucket, err error)
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context) ([]api.ContractMetadata, error)
	ContractSetContracts(ctx context.Context, set string) (contracts []api.ContractMetadata, err error)
	CreateBucket(ctx context.Context, bucketName string, opts api.CreateBucketOptions) error
	DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error
	GougingParams(ctx context.Context) (api.GougingParams, error)
	ListBuckets(ctx context.Context) (buckets []api.Bucket, err error)
	ListObjects(ctx context.Context, bucket string, opts api.ListObjectOptions) (resp api.ObjectsListResponse, err error)
	Object(ctx context.Context, bucket, path string, options api.GetObjectOptions) (api.ObjectsResponse, error)
	FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
	Slab(ctx context.Context, key object.EncryptionKey) (slab object.Slab, err error)
	UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error
	UploadPackingSettings(ctx context.Context) (ups api.UploadPackingSettings, err error)
}

// workerClient is the interface for renterd/worker.
type workerClient interface {
	Contracts(ctx context.Context, hostTimeout time.Duration) (resp api.ContractsResponse, err error)
}

// Satellite is the interface between the renting software and the
// Sia Satellite node.
type Satellite struct {
	ap         autopilotClient
	bus        busClient
	worker     workerClient
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
func NewSatellite(ac autopilotClient, bc busClient, wc workerClient, dir string, seed types.PrivateKey, l *zap.Logger, satAddr string, satPassword string) (http.Handler, error) {
	satelliteDir := filepath.Join(dir, "satellite")
	if err := os.MkdirAll(satelliteDir, 0700); err != nil {
		return nil, err
	}
	ss, err := newJSONStore(satelliteDir)
	if err != nil {
		return nil, err
	}

	s, err := New(ac, bc, wc, *ss, seed, l)
	if err != nil {
		return nil, err
	}

	// Initialize the client.
	StaticSatellite = NewClient(satAddr, satPassword)

	return s.Handler(), nil
}

// New returns a new Satellite.
func New(ac autopilotClient, bc busClient, wc workerClient, ss jsonStore, seed types.PrivateKey, l *zap.Logger) (*Satellite, error) {
	s := &Satellite{
		ap:         ac,
		bus:        bc,
		worker:     wc,
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

	// Opt out of all features before disabling or changing satellite.
	cfg := s.store.getConfig()
	if cfg.Enabled && (!sc.Enabled || cfg.PublicKey != sc.PublicKey) {
		ctx := jc.Request.Context()
		StaticSatellite.UpdateSettings(ctx, RenterSettings{})
	}

	if jc.Check("failed to set config", s.store.setConfig(sc)) != nil {
		return
	}

	// Exchange contracts with the satellite if it was enabled or changed.
	if sc.Enabled && (!cfg.Enabled || cfg.PublicKey != sc.PublicKey) {
		go func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			StaticSatellite.ShareContracts(ctx)
			StaticSatellite.RequestContracts(ctx)
		}()
	}
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

// objectHandlerGET handles the GET /object requests.
func (s *Satellite) objectHandlerGET(jc jape.Context) {
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	parts, exists := s.store.getObject(bucket, strings.TrimPrefix(jc.PathParam("path"), "/"))
	or := ObjectResponse{
		Found: exists,
		Parts: parts,
	}
	jc.Encode(or)
}

// objectHandlerPUT handles the PUT /object request.
func (s *Satellite) objectHandlerPUT(jc jape.Context) {
	var opr ObjectPutRequest
	if jc.Decode(&opr) != nil {
		return
	}
	jc.Check("failed to add object to the store", s.store.addObject(opr.Bucket, strings.TrimPrefix(jc.PathParam("path"), "/"), opr.Parts))
}

// objectHandlerDELETE handles the DELETE /object request.
func (s *Satellite) objectHandlerDELETE(jc jape.Context) {
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	jc.Check("failed to delete object from the store", s.store.deleteObject(bucket, strings.TrimPrefix(jc.PathParam("path"), "/")))
}

// objectsHandlerDELETE handles the DELETE /objects request.
func (s *Satellite) objectsHandlerDELETE(jc jape.Context) {
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	jc.Check("failed to delete objects from the store", s.store.deleteObjects(bucket, strings.TrimPrefix(jc.PathParam("path"), "/")))
}

// Handler returns an HTTP handler that serves the satellite API.
func (s *Satellite) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /request":            s.requestContractsHandler,
		"POST   /contracts":          s.shareContractsHandler,
		"POST   /form":               s.formContractsHandler,
		"POST   /renew":              s.renewContractsHandler,
		"POST   /update":             s.updateRevisionHandler,
		"GET    /config":             s.configHandlerGET,
		"PUT    /config":             s.configHandlerPUT,
		"PUT    /satellite":          s.satelliteHandlerPUT,
		"GET    /satellite/:id":      s.satelliteHandlerGET,
		"GET    /satellites":         s.satellitesHandlerGET,
		"GET    /object/*path":       s.objectHandlerGET,
		"PUT    /object/*path":       s.objectHandlerPUT,
		"DELETE /object/*path":       s.objectHandlerDELETE,
		"DELETE /objects/*path":      s.objectsHandlerDELETE,
		"POST   /rspv2/form":         s.formContractHandler,
		"POST   /rspv2/renew":        s.renewContractHandler,
		"GET    /settings":           s.settingsHandlerGET,
		"POST   /settings":           s.settingsHandlerPOST,
		"POST   /metadata":           s.saveMetadataHandler,
		"GET    /metadata/:set":      s.requestMetadataHandler,
		"GET    /slabs/:set":         s.requestSlabsHandler,
		"POST   /slab":               s.updateSlabHandler,
		"POST   /multipart/create":   s.createMultipartHandler,
		"POST   /multipart/abort":    s.abortMultipartHandler,
		"POST   /multipart/complete": s.completeMultipartHandler,
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
	parseEnvVar("RENTERD_SATELLITE_MUX", &cfg.MuxPort)
	parseEnvVar("RENTERD_SATELLITE_KEY", &cfg.PublicKey)
	parseEnvVar("RENTERD_SATELLITE_SEED", &cfg.RenterSeed)
}
