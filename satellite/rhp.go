package satellite

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"

	"golang.org/x/crypto/blake2b"
)

const (
	// timeoutHostRevision is the amount of time we wait to receive the latest
	// revision from the host.
	timeoutHostRevision = 15 * time.Second
)

var (
	specifierRequestContracts = types.NewSpecifier("RequestContracts")
	specifierFormContracts    = types.NewSpecifier("FormContracts")
	specifierRenewContracts   = types.NewSpecifier("RenewContracts")
	specifierUpdateRevision   = types.NewSpecifier("UpdateRevision")
	specifierFormContract     = types.NewSpecifier("FormContract")
	specifierRenewContract    = types.NewSpecifier("RenewContract")
	specifierGetSettings      = types.NewSpecifier("GetSettings")
	specifierUpdateSettings   = types.NewSpecifier("UpdateSettings")
	specifierSaveMetadata     = types.NewSpecifier("SaveMetadata")
	specifierRequestMetadata  = types.NewSpecifier("RequestMetadata")
	specifierUpdateSlab       = types.NewSpecifier("UpdateSlab")
	specifierShareContracts   = types.NewSpecifier("ShareContracts")
)

// requestRequest is used to request existing contracts.
type requestRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
}

// formRequest is used to request contract formation.
type formRequest struct {
	PubKey      types.PublicKey
	SecretKey   types.PrivateKey
	Hosts       uint64
	Period      uint64
	RenewWindow uint64

	Storage  uint64
	Upload   uint64
	Download uint64

	MinShards   uint64
	TotalShards uint64

	MaxRPCPrice          types.Currency
	MaxContractPrice     types.Currency
	MaxDownloadPrice     types.Currency
	MaxUploadPrice       types.Currency
	MaxStoragePrice      types.Currency
	MaxSectorAccessPrice types.Currency
	MinMaxCollateral     types.Currency
	BlockHeightLeeway    uint64

	Signature types.Signature
}

// formContractRequest is used to request contract formation using
// the new Renter-Satellite protocol.
type formContractRequest struct {
	PubKey    types.PublicKey
	RenterKey types.PublicKey
	HostKey   types.PublicKey
	EndHeight uint64

	Storage  uint64
	Upload   uint64
	Download uint64

	MinShards   uint64
	TotalShards uint64

	Signature types.Signature
}

// renewRequest is used to request contract renewal.
type renewRequest struct {
	PubKey      types.PublicKey
	SecretKey   types.PrivateKey
	Contracts   []types.FileContractID
	Period      uint64
	RenewWindow uint64

	Storage  uint64
	Upload   uint64
	Download uint64

	MinShards   uint64
	TotalShards uint64

	MaxRPCPrice          types.Currency
	MaxContractPrice     types.Currency
	MaxDownloadPrice     types.Currency
	MaxUploadPrice       types.Currency
	MaxStoragePrice      types.Currency
	MaxSectorAccessPrice types.Currency
	MinMaxCollateral     types.Currency
	BlockHeightLeeway    uint64

	Signature types.Signature
}

// renewContractRequest is used to request contract renewal using
// the new Renter-Satellite protocol.
type renewContractRequest struct {
	PubKey    types.PublicKey
	Contract  types.FileContractID
	EndHeight uint64

	Storage  uint64
	Upload   uint64
	Download uint64

	MinShards   uint64
	TotalShards uint64

	Signature types.Signature
}

// revisionHash is used to read the revision hash provided by the
// satellite.
type revisionHash struct {
	RevisionHash types.Hash256
}

// renterSignature is used to send the revision signature to the
// satellite.
type renterSignature struct {
	Signature types.Signature
}

// updateRequest is used to send a new revision.
type updateRequest struct {
	PubKey      types.PublicKey
	Contract    rhpv2.ContractRevision
	Uploads     types.Currency
	Downloads   types.Currency
	FundAccount types.Currency
	Signature   types.Signature
}

// extendedContract contains the contract and its metadata.
type extendedContract struct {
	contract            rhpv2.ContractRevision
	startHeight         uint64
	totalCost           types.Currency
	uploadSpending      types.Currency
	downloadSpending    types.Currency
	fundAccountSpending types.Currency
	renewedFrom         types.FileContractID
}

// extendedContractSet is a collection of extendedContracts.
type extendedContractSet struct {
	contracts []extendedContract
}

// getSettingsRequest is used to retrieve the renter's opt-in settings.
type getSettingsRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
}

// updateSettingsRequest is used to update the renter's opt-in settings.
type updateSettingsRequest struct {
	PubKey             types.PublicKey
	AutoRenewContracts bool
	BackupFileMetadata bool
	AutoRepairFiles    bool
	SecretKey          types.PrivateKey
	AccountKey         types.PrivateKey

	Hosts       uint64
	Period      uint64
	RenewWindow uint64
	Storage     uint64
	Upload      uint64
	Download    uint64
	MinShards   uint64
	TotalShards uint64

	MaxRPCPrice          types.Currency
	MaxContractPrice     types.Currency
	MaxDownloadPrice     types.Currency
	MaxUploadPrice       types.Currency
	MaxStoragePrice      types.Currency
	MaxSectorAccessPrice types.Currency
	MinMaxCollateral     types.Currency
	BlockHeightLeeway    uint64

	Signature types.Signature
}

// saveMetadataRequest is used to save file metadata on the satellite.
type saveMetadataRequest struct {
	PubKey    types.PublicKey
	Metadata  FileMetadata
	Signature types.Signature
}

// renterFiles is a collection of FileMetadata.
type renterFiles struct {
	metadata []FileMetadata
}

// requestMetadataRequest is used to request file metadata.
type requestMetadataRequest struct {
	PubKey         types.PublicKey
	PresentObjects []BucketFiles
	Signature      types.Signature
}

// updateSlabRequest is used to update a slab after a successful migration.
type updateSlabRequest struct {
	PubKey    types.PublicKey
	Slab      object.SlabSlice
	Signature types.Signature
}

// shareRequest is used to send a set of contracts to the satellite.
type shareRequest struct {
	PubKey    types.PublicKey
	Contracts []api.Contract
	Signature types.Signature
}

// generateKeyPair generates the keypair from a given seed.
func generateKeyPair(seed []byte) (types.PublicKey, types.PrivateKey) {
	privKey := types.NewPrivateKeyFromSeed(seed)
	return privKey.PublicKey(), privKey
}

// deriveRenterKey derives a subkey to be used for signing the transaction
// signature when forming a contract.
func (s *Satellite) deriveRenterKey(hostKey types.PublicKey) types.PrivateKey {
	seed := blake2b.Sum256(append(s.renterKey, hostKey[:]...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// requestContractsHandler handles the /request requests.
func (s *Satellite) requestContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't request contracts: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	s.logger.Info("requesting contracts from the satellite")
	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(cfg.RenterSeed)

	rr := requestRequest{
		PubKey: pk,
	}

	h := types.NewHasher()
	rr.EncodeToWithoutSignature(h.E)
	rr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err := s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRequestContracts, &rr); err != nil {
			return err
		}

		if err := t.ReadResponse(&ecs, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't request contracts", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't request contracts: %s", err))
		return
	}

	var added []api.ContractMetadata
	var contracts []types.FileContractID
	fcids := make(map[types.FileContractID]struct{})

	existing, _ := s.bus.Contracts(ctx)
	for _, c := range existing {
		contracts = append(contracts, c.ID)
		fcids[c.ID] = struct{}{}
	}

	var recs []api.ContractSpendingRecord
	for _, ec := range ecs.contracts {
		id := ec.contract.ID()
		if _, exists := fcids[id]; exists {
			continue // only add the contract if it's not in the database
		}
		contracts = append(contracts, id)
		var a api.ContractMetadata
		if (ec.renewedFrom == types.FileContractID{}) {
			a, err = s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight)
		} else {
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.totalCost, ec.startHeight, ec.renewedFrom)
			if err != nil {
				// there might be no old contract in the archive, add as a new contract
				a, err = s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight)
			}
		}
		if jc.Check("couldn't add contract", err) != nil {
			s.logger.Error(fmt.Sprintf("couldn't add requested contract: %s", err))
			return
		}
		added = append(added, a)
		recs = append(recs, api.ContractSpendingRecord{
			ContractSpending: api.ContractSpending{
				Uploads:     ec.uploadSpending,
				Downloads:   ec.downloadSpending,
				FundAccount: ec.fundAccountSpending,
			},
			ContractID: id,
		})
	}
	err = s.bus.RecordContractSpending(ctx, recs)
	if jc.Check("couldn't update contract spendings", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't update contract spendings: %s", err))
		return
	}

	err = s.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't set contract set: %s", err))
		return
	}

	s.logger.Info(fmt.Sprintf("successfully added %v new contracts", len(added)))
	jc.Encode(added)
}

// formContractsHandler handles the /form requests.
func (s *Satellite) formContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't form contracts: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var sfr FormRequest
	if jc.Decode(&sfr) != nil {
		return
	}

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	fr := formRequest{
		PubKey:      pk,
		SecretKey:   s.renterKey,
		Hosts:       sfr.Hosts,
		Period:      sfr.Period,
		RenewWindow: sfr.RenewWindow,

		Storage:  sfr.Storage,
		Upload:   sfr.Upload,
		Download: sfr.Download,

		MinShards:   uint64(gp.RedundancySettings.MinShards),
		TotalShards: uint64(gp.RedundancySettings.TotalShards),

		MaxRPCPrice:          gp.GougingSettings.MaxRPCPrice,
		MaxContractPrice:     gp.GougingSettings.MaxContractPrice,
		MaxDownloadPrice:     gp.GougingSettings.MaxDownloadPrice,
		MaxUploadPrice:       gp.GougingSettings.MaxUploadPrice,
		MaxStoragePrice:      gp.GougingSettings.MaxStoragePrice,
		MaxSectorAccessPrice: gp.GougingSettings.MaxRPCPrice.Mul64(10),
		MinMaxCollateral:     gp.GougingSettings.MinMaxCollateral,
		BlockHeightLeeway:    uint64(gp.GougingSettings.HostBlockHeightLeeway),
	}

	s.logger.Debug(fmt.Sprintf("trying to form %v contracts", fr.Hosts))

	h := types.NewHasher()
	fr.EncodeToWithoutSignature(h.E)
	fr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierFormContracts, &fr); err != nil {
			return err
		}

		if err := t.ReadResponse(&ecs, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't form contracts", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't form contracts: %s", err))
		return
	}

	var added []api.ContractMetadata
	var contracts []types.FileContractID

	existing, _ := s.bus.Contracts(ctx)
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	for _, ec := range ecs.contracts {
		id := ec.contract.ID()
		contracts = append(contracts, id)
		a, err := s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight)
		if jc.Check("couldn't add contract", err) != nil {
			s.logger.Error(fmt.Sprintf("couldn't add contract: %s", err))
			return
		}
		added = append(added, a)
	}
	err = s.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't set contract set: %s", err))
		return
	}

	s.logger.Info(fmt.Sprintf("successfully added %v new contracts", len(added)))
	jc.Encode(added)
}

// renewContractsHandler handles the /renew requests.
func (s *Satellite) renewContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't renew contracts: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var srr RenewRequest
	if jc.Decode(&srr) != nil {
		return
	}

	renewedFrom := make(map[types.PublicKey]types.FileContractID)
	for _, id := range srr.Contracts {
		contract, err := s.bus.Contract(ctx, id)
		if err != nil {
			continue
		}
		renewedFrom[contract.HostKey] = id
	}

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	rr := renewRequest{
		PubKey:      pk,
		SecretKey:   s.renterKey,
		Contracts:   srr.Contracts,
		Period:      srr.Period,
		RenewWindow: srr.RenewWindow,

		Storage:  srr.Storage,
		Upload:   srr.Upload,
		Download: srr.Download,

		MinShards:   uint64(gp.RedundancySettings.MinShards),
		TotalShards: uint64(gp.RedundancySettings.TotalShards),

		MaxRPCPrice:          gp.GougingSettings.MaxRPCPrice,
		MaxContractPrice:     gp.GougingSettings.MaxContractPrice,
		MaxDownloadPrice:     gp.GougingSettings.MaxDownloadPrice,
		MaxUploadPrice:       gp.GougingSettings.MaxUploadPrice,
		MaxStoragePrice:      gp.GougingSettings.MaxStoragePrice,
		MaxSectorAccessPrice: gp.GougingSettings.MaxRPCPrice.Mul64(10),
		MinMaxCollateral:     gp.GougingSettings.MinMaxCollateral,
		BlockHeightLeeway:    uint64(gp.GougingSettings.HostBlockHeightLeeway),
	}

	s.logger.Debug(fmt.Sprintf("trying to renew %v contracts", len(rr.Contracts)))

	h := types.NewHasher()
	rr.EncodeToWithoutSignature(h.E)
	rr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRenewContracts, &rr); err != nil {
			return err
		}

		if err := t.ReadResponse(&ecs, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't renew contracts", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't renew contracts: %s", err))
		return
	}

	var added []api.ContractMetadata

	for _, ec := range ecs.contracts {
		host := ec.contract.HostKey()
		from, ok := renewedFrom[host]
		var a api.ContractMetadata
		if ok {
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.totalCost, ec.startHeight, from)
		} else {
			a, err = s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight)
		}
		if jc.Check("couldn't add contract", err) != nil {
			s.logger.Error(fmt.Sprintf("couldn't add contract: %s", err))
			return
		}
		added = append(added, a)
	}

	s.logger.Info(fmt.Sprintf("successfully renewed %v contracts", len(added)))
	jc.Encode(added)
}

// updateRevisionHandler submits an updated contract revision to the satellite.
func (s *Satellite) updateRevisionHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var sur UpdateRevisionRequest
	if jc.Decode(&sur) != nil {
		return
	}

	cfg := s.store.getConfig()
	if !cfg.Enabled {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	ur := updateRequest{
		PubKey:      pk,
		Contract:    sur.Revision,
		Uploads:     sur.Spending.Uploads,
		Downloads:   sur.Spending.Downloads,
		FundAccount: sur.Spending.FundAccount,
	}

	h := types.NewHasher()
	ur.EncodeToWithoutSignature(h.E)
	ur.Signature = sk.SignHash(h.Sum())

	conn, err := dial(ctx, cfg.Address, cfg.PublicKey)
	if jc.Check("could not connect to the satellite", err) != nil {
		return
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()

	defer func() {
		close(done)
		if ctx.Err() != nil {
			jc.Check("ERROR:", ctx.Err())
		}
	}()

	t, err := rhpv2.NewRenterTransport(conn, cfg.PublicKey)
	if jc.Check("could not create transport", err) != nil {
		return
	}
	defer t.Close()

	err = t.WriteRequest(specifierUpdateRevision, &ur)
	if jc.Check("could not write request", err) != nil {
		return
	}

	var resp rhpv2.RPCError
	err = t.ReadResponse(&resp, 1024)
	if jc.Check("could not read response", err) != nil {
		return
	}

	if resp.Description != "" {
		jc.Check("ERROR:", errors.New(resp.Description))
	}
}

// formContractHandler handles the /rspv2/form requests.
func (s *Satellite) formContractHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't form a contract: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var sfr FormContractRequest
	if jc.Decode(&sfr) != nil {
		return
	}

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)
	renterKey := s.deriveRenterKey(sfr.HostKey)

	fcr := formContractRequest{
		PubKey:    pk,
		RenterKey: renterKey.PublicKey(),
		HostKey:   sfr.HostKey,
		EndHeight: sfr.EndHeight,

		Storage:  sfr.Storage,
		Upload:   sfr.Upload,
		Download: sfr.Download,

		MinShards:   uint64(gp.RedundancySettings.MinShards),
		TotalShards: uint64(gp.RedundancySettings.TotalShards),
	}

	s.logger.Debug(fmt.Sprintf("trying to form a contract with %s", sfr.HostKey))

	h := types.NewHasher()
	fcr.EncodeToWithoutSignature(h.E)
	fcr.Signature = sk.SignHash(h.Sum())

	var ec extendedContract
	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		// Write the FormContract request.
		if err := t.WriteRequest(specifierFormContract, &fcr); err != nil {
			return err
		}

		// Read the revision hash.
		var rh revisionHash
		if err := t.ReadResponse(&rh, 65536); err != nil {
			return err
		}

		// Sign the hash and send the signature to the satellite.
		rs := &renterSignature{
			Signature: renterKey.SignHash(rh.RevisionHash),
		}
		if err := t.WriteResponse(rs); err != nil {
			return err
		}

		// Read the contract.
		if err := t.ReadResponse(&ec, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't form a contract", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't form a contract with %s: %s", sfr.HostKey, err))
		return
	}

	var contracts []types.FileContractID

	existing, _ := s.bus.Contracts(ctx)
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	id := ec.contract.ID()
	contracts = append(contracts, id)
	added, err := s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight)
	if jc.Check("couldn't add contract", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't add contract: %s", err))
		return
	}

	err = s.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't set contract set: %s", err))
		return
	}

	s.logger.Debug(fmt.Sprintf("successfully added new contract with %s", sfr.HostKey))
	jc.Encode(added)
}

// renewContractHandler handles the /rspv2/renew requests.
func (s *Satellite) renewContractHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't renew contract: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var srr RenewContractRequest
	if jc.Decode(&srr) != nil {
		return
	}

	contract, err := s.bus.Contract(ctx, srr.Contract)
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)
	renterKey := s.deriveRenterKey(contract.HostKey)

	rcr := renewContractRequest{
		PubKey:    pk,
		Contract:  srr.Contract,
		EndHeight: srr.EndHeight,

		Storage:  srr.Storage,
		Upload:   srr.Upload,
		Download: srr.Download,

		MinShards:   uint64(gp.RedundancySettings.MinShards),
		TotalShards: uint64(gp.RedundancySettings.TotalShards),
	}

	s.logger.Debug(fmt.Sprintf("trying to renew a contract with %s", contract.HostKey))

	h := types.NewHasher()
	rcr.EncodeToWithoutSignature(h.E)
	rcr.Signature = sk.SignHash(h.Sum())

	var ec extendedContract
	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRenewContract, &rcr); err != nil {
			return err
		}

		// Read the old revision hash.
		var rh revisionHash
		if err := t.ReadResponse(&rh, 65536); err != nil {
			return err
		}

		// Sign the hash and send the signature to the satellite.
		rs := &renterSignature{
			Signature: renterKey.SignHash(rh.RevisionHash),
		}
		if err := t.WriteResponse(rs); err != nil {
			return err
		}

		// Read the new revision hash.
		if err := t.ReadResponse(&rh, 65536); err != nil {
			return err
		}

		// Sign the hash and send the signature to the satellite.
		rs.Signature = renterKey.SignHash(rh.RevisionHash)
		if err := t.WriteResponse(rs); err != nil {
			return err
		}

		// Read the new contract.
		if err := t.ReadResponse(&ec, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't renew contract", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't renew contract with %s: %s", contract.HostKey, err))
		return
	}

	added, err := s.bus.AddRenewedContract(ctx, ec.contract, ec.totalCost, ec.startHeight, srr.Contract)
	if jc.Check("couldn't add contract", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't add contract: %s", err))
		return
	}

	s.logger.Debug(fmt.Sprintf("successfully renewed contract with %s", contract.HostKey))
	jc.Encode(added)
}

// settingsHandlerGET handles the GET /settings requests.
func (s *Satellite) settingsHandlerGET(jc jape.Context) {
	var settings RenterSettings
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		jc.Encode(settings)
		return
	}

	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(cfg.RenterSeed)

	gsr := getSettingsRequest{
		PubKey: pk,
	}

	h := types.NewHasher()
	gsr.EncodeToWithoutSignature(h.E)
	gsr.Signature = sk.SignHash(h.Sum())

	err := s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierGetSettings, &gsr); err != nil {
			return err
		}

		if err := t.ReadResponse(&settings, 4096); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't retrieve settings", err) != nil {
		return
	}

	jc.Encode(settings)
}

// settingsHandlerPOST handles the POST /settings requests.
func (s *Satellite) settingsHandlerPOST(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		return
	}

	ctx := jc.Request.Context()
	var settings RenterSettings
	if jc.Decode(&settings) != nil {
		return
	}

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	ac, err := s.ap.Config()
	if jc.Check("could not get autopilot config", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	usr := updateSettingsRequest{
		PubKey:             pk,
		AutoRenewContracts: settings.AutoRenewContracts,
		BackupFileMetadata: settings.BackupFileMetadata,
		AutoRepairFiles:    settings.AutoRepairFiles,
		SecretKey:          s.renterKey,
		AccountKey:         s.accountKey,

		Hosts:       ac.Contracts.Amount,
		Period:      ac.Contracts.Period,
		RenewWindow: ac.Contracts.RenewWindow,

		Storage:  ac.Contracts.Storage,
		Upload:   ac.Contracts.Upload,
		Download: ac.Contracts.Download,

		MinShards:   uint64(gp.RedundancySettings.MinShards),
		TotalShards: uint64(gp.RedundancySettings.TotalShards),

		MaxRPCPrice:          gp.GougingSettings.MaxRPCPrice,
		MaxContractPrice:     gp.GougingSettings.MaxContractPrice,
		MaxDownloadPrice:     gp.GougingSettings.MaxDownloadPrice,
		MaxUploadPrice:       gp.GougingSettings.MaxUploadPrice,
		MaxStoragePrice:      gp.GougingSettings.MaxStoragePrice,
		MaxSectorAccessPrice: gp.GougingSettings.MaxRPCPrice.Mul64(10),
		MinMaxCollateral:     gp.GougingSettings.MinMaxCollateral,
		BlockHeightLeeway:    uint64(gp.GougingSettings.HostBlockHeightLeeway),
	}

	h := types.NewHasher()
	usr.EncodeToWithoutSignature(h.E)
	usr.Signature = sk.SignHash(h.Sum())

	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		return t.WriteRequest(specifierUpdateSettings, &usr)
	})

	if jc.Check("couldn't update settings", err) != nil {
		return
	}

	// Transfer all file metadata if backups are enabled.
	if settings.BackupFileMetadata {
		s.transferMetadata(ctx)
	}
}

// transferMetadata sends all file metadata to the satellite.
func (s *Satellite) transferMetadata(ctx context.Context) {
	buckets, err := s.bus.ListBuckets(ctx)
	if err != nil {
		s.logger.Error(fmt.Sprintf("couldn't get buckets: %s", err))
		return
	}
	for _, bucket := range buckets {
		resp, err := s.bus.ListObjects(ctx, bucket.Name, api.ListObjectOptions{
			Limit: -1,
		})
		if err != nil {
			s.logger.Error(fmt.Sprintf("couldn't get bucket objects: %s: %s", bucket.Name, err))
			continue
		}
		for _, entry := range resp.Objects {
			resp, err := s.bus.Object(ctx, bucket.Name, entry.Name, api.GetObjectOptions{})
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't find object %s: %s", entry.Name, err))
				continue
			}
			StaticSatellite.SaveMetadata(ctx, FileMetadata{
				Key:      resp.Object.Key,
				Bucket:   bucket.Name,
				Path:     entry.Name,
				ETag:     resp.Object.ETag,
				MimeType: resp.Object.MimeType,
				Slabs:    resp.Object.Slabs,
			})
		}
	}
}

// saveMetadataHandler handles the POST /metadata requests.
func (s *Satellite) saveMetadataHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		return
	}
	ctx := jc.Request.Context()
	var fmr SaveMetadataRequest
	if jc.Decode(&fmr) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	smr := saveMetadataRequest{
		PubKey:   pk,
		Metadata: fmr.Metadata,
	}

	h := types.NewHasher()
	smr.EncodeToWithoutSignature(h.E)
	smr.Signature = sk.SignHash(h.Sum())

	err := s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		err = t.WriteRequest(specifierSaveMetadata, &smr)
		if err != nil {
			return
		}

		var resp rhpv2.RPCError
		err = t.ReadResponse(&resp, 1024)
		if jc.Check("could not read response", err) != nil {
			return
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})

	if jc.Check("couldn't save metadata", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't save file metadata: %s", err))
	}
}

// requestMetadataHandler handles the GET /metadata requests.
func (s *Satellite) requestMetadataHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't request file metadata: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	set := jc.PathParam("set")
	if set == "" {
		jc.Check("ERROR", errors.New("contract set cannot be empty"))
		return
	}
	s.logger.Info("requesting file metadata from the satellite")
	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(cfg.RenterSeed)
	rmr := requestMetadataRequest{
		PubKey: pk,
	}

	buckets, err := s.bus.ListBuckets(ctx)
	if jc.Check("couldn't get buckets", err) != nil {
		return
	}
	for _, bucket := range buckets {
		resp, err := s.bus.ListObjects(ctx, bucket.Name, api.ListObjectOptions{
			Limit: -1,
		})
		if jc.Check("couldn't requests present objects", err) != nil {
			return
		}
		bf := BucketFiles{
			Name: bucket.Name,
		}
		for _, entry := range resp.Objects {
			bf.Paths = append(bf.Paths, entry.Name)
		}
		rmr.PresentObjects = append(rmr.PresentObjects, bf)
	}

	h := types.NewHasher()
	rmr.EncodeToWithoutSignature(h.E)
	rmr.Signature = sk.SignHash(h.Sum())

	var rf renterFiles
	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRequestMetadata, &rmr); err != nil {
			return err
		}

		if err := t.ReadResponse(&rf, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't request file metadata", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't request file metadata: %s", err))
		return
	}

	contracts, err := s.bus.ContractSetContracts(ctx, set)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	var objects []object.Object
	for _, fm := range rf.metadata {
		obj := object.Object{
			Key:   fm.Key,
			Slabs: fm.Slabs,
		}
		h2c := make(map[types.PublicKey]types.FileContractID)
		for _, c := range contracts {
			h2c[c.HostKey] = c.ID
		}
		used := make(map[types.PublicKey]types.FileContractID)
		for _, s := range obj.Slabs {
			for _, ss := range s.Shards {
				used[ss.Host] = h2c[ss.Host]
			}
		}
		_, err := s.bus.Bucket(ctx, fm.Bucket)
		if err != nil {
			err = s.bus.CreateBucket(ctx, fm.Bucket, api.CreateBucketOptions{})
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't create bucket: %s", err))
				continue
			}
		}
		_, err = s.bus.Object(ctx, fm.Bucket, fm.Path, api.GetObjectOptions{})
		if err == nil {
			err = s.bus.DeleteObject(ctx, fm.Bucket, fm.Path, api.DeleteObjectOptions{})
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't delete object: %s", err))
				continue
			}
		}
		if err := s.bus.AddObject(ctx, fm.Bucket, fm.Path, set, obj, used, api.AddObjectOptions{
			ETag:     fm.ETag,
			MimeType: fm.MimeType,
		}); err != nil {
			s.logger.Error(fmt.Sprintf("couldn't add object: %s", err))
			continue
		}
		objects = append(objects, obj)
	}

	s.logger.Info(fmt.Sprintf("successfully added %v objects", len(objects)))
	jc.Encode(objects)
}

// updateSlabHandler handles the POST /slab requests.
func (s *Satellite) updateSlabHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		return
	}
	ctx := jc.Request.Context()
	var req UpdateSlabRequest
	if jc.Decode(&req) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	usr := updateSlabRequest{
		PubKey: pk,
		Slab: object.SlabSlice{
			Slab:   req.Slab,
			Offset: 0,
			Length: 0,
		},
	}

	h := types.NewHasher()
	usr.EncodeToWithoutSignature(h.E)
	usr.Signature = sk.SignHash(h.Sum())

	err := s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		err = t.WriteRequest(specifierUpdateSlab, &usr)
		if err != nil {
			return
		}

		var resp rhpv2.RPCError
		err = t.ReadResponse(&resp, 1024)
		if jc.Check("could not read response", err) != nil {
			return
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})

	if jc.Check("couldn't update slab", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't update slab: %s", err))
	}
}

// shareContractsHandler handles the POST /contracts requests.
func (s *Satellite) shareContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		return
	}
	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(cfg.RenterSeed)

	resp, err := s.worker.Contracts(ctx, timeoutHostRevision)
	if err != nil {
		s.logger.Error(fmt.Sprintf("couldn't fetch contracts: %s", err))
		return
	}
	if resp.Error != "" {
		s.logger.Error(resp.Error)
	}

	sr := shareRequest{
		PubKey: pk,
	}
	for _, contract := range resp.Contracts {
		if contract.Revision != nil {
			sr.Contracts = append(sr.Contracts, contract)
		}
	}

	h := types.NewHasher()
	sr.EncodeToWithoutSignature(h.E)
	sr.Signature = sk.SignHash(h.Sum())

	err = s.withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		err = t.WriteRequest(specifierShareContracts, &sr)
		if err != nil {
			return
		}

		var resp rhpv2.RPCError
		err = t.ReadResponse(&resp, 1024)
		if err != nil {
			s.logger.Error(fmt.Sprintf("could not read response: %s", err))
			return
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})

	if err != nil {
		s.logger.Error(fmt.Sprintf("couldn't send contracts: %s", err))
	}
}
