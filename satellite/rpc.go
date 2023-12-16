package satellite

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/satellite/encrypt"

	"golang.org/x/crypto/blake2b"
)

const (
	// timeoutHostRevision is the amount of time we wait to receive the latest
	// revision from the host.
	timeoutHostRevision = 15 * time.Second
)

var (
	specifierRequestContracts  = types.NewSpecifier("RequestContracts")
	specifierFormContracts     = types.NewSpecifier("FormContracts")
	specifierRenewContracts    = types.NewSpecifier("RenewContracts")
	specifierUpdateRevision    = types.NewSpecifier("UpdateRevision")
	specifierFormContract      = types.NewSpecifier("FormContract")
	specifierRenewContract     = types.NewSpecifier("RenewContract")
	specifierGetSettings       = types.NewSpecifier("GetSettings")
	specifierUpdateSettings    = types.NewSpecifier("UpdateSettings")
	specifierSaveMetadata      = types.NewSpecifier("SaveMetadata")
	specifierRequestMetadata   = types.NewSpecifier("RequestMetadata")
	specifierUpdateSlab        = types.NewSpecifier("UpdateSlab")
	specifierRequestSlabs      = types.NewSpecifier("RequestSlabs")
	specifierShareContracts    = types.NewSpecifier("ShareContracts")
	specifierUploadFile        = types.NewSpecifier("UploadFile")
	specifierCreateMultipart   = types.NewSpecifier("CreateMultipart")
	specifierAbortMultipart    = types.NewSpecifier("AbortMultipart")
	specifierUploadPart        = types.NewSpecifier("UploadPart")
	specifierCompleteMultipart = types.NewSpecifier("FinishMultipart")
)

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
	err := withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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
			a, err = s.bus.AddContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, api.ContractStateActive)
		} else {
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, ec.renewedFrom, api.ContractStateActive)
			if err != nil {
				// there might be no old contract in the archive, add as a new contract
				a, err = s.bus.AddContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, api.ContractStateActive)
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

	ups, err := s.bus.UploadPackingSettings(ctx)
	if jc.Check("could not get upload packing settings", err) != nil {
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

		UploadPacking: ups.Enabled,
	}

	s.logger.Debug(fmt.Sprintf("trying to form %v contracts", fr.Hosts))

	h := types.NewHasher()
	fr.EncodeToWithoutSignature(h.E)
	fr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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
		a, err := s.bus.AddContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, api.ContractStateActive)
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

	ups, err := s.bus.UploadPackingSettings(ctx)
	if jc.Check("could not get upload packing settings", err) != nil {
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

		UploadPacking: ups.Enabled,
	}

	s.logger.Debug(fmt.Sprintf("trying to renew %v contracts", len(rr.Contracts)))

	h := types.NewHasher()
	rr.EncodeToWithoutSignature(h.E)
	rr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, from, api.ContractStateActive)
		} else {
			a, err = s.bus.AddContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, api.ContractStateActive)
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

	conn, err := dial(ctx, cfg.Address)
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
	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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
	added, err := s.bus.AddContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, api.ContractStateActive)
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
	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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

	added, err := s.bus.AddRenewedContract(ctx, ec.contract, ec.contractPrice, ec.totalCost, ec.startHeight, srr.Contract, api.ContractStateActive)
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

	err := withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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

	ups, err := s.bus.UploadPackingSettings(ctx)
	if jc.Check("could not get upload packing settings", err) != nil {
		return
	}

	ac, err := s.ap.Config()
	if jc.Check("could not get autopilot config", err) != nil {
		return
	}

	rs, err := StaticSatellite.GetSettings(ctx)
	if jc.Check("could not retrieve current settings", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	usr := updateSettingsRequest{
		PubKey:             pk,
		AutoRenewContracts: settings.AutoRenewContracts,
		BackupFileMetadata: settings.BackupFileMetadata,
		AutoRepairFiles:    settings.AutoRepairFiles,
		ProxyUploads:       settings.ProxyUploads,
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

		UploadPacking: ups.Enabled,
	}

	h := types.NewHasher()
	usr.EncodeToWithoutSignature(h.E)
	usr.Signature = sk.SignHash(h.Sum())

	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		return t.WriteRequest(specifierUpdateSettings, &usr)
	})

	if jc.Check("couldn't update settings", err) != nil {
		return
	}

	// Transfer all file metadata if backups are enabled.
	if settings.BackupFileMetadata && !rs.BackupFileMetadata {
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
			var partialSlabData []byte
			for _, slab := range resp.Object.Slabs {
				if !slab.IsPartial() {
					continue
				}
				data, err := s.bus.FetchPartialSlab(ctx, slab.Key, slab.Offset, slab.Length)
				if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
					// Check if the slab was already uploaded.
					ss, err := s.bus.Slab(ctx, slab.Key)
					if err != nil {
						s.logger.Error(fmt.Sprintf("failed to fetch uploaded partial slab: %v", err))
						continue
					}
					resp.Object.Slabs = append(resp.Object.Slabs, object.SlabSlice{
						Slab:   ss,
						Offset: slab.Offset,
						Length: slab.Length,
					})
				} else if err != nil {
					s.logger.Error(fmt.Sprintf("failed to fetch partial slab: %v", err))
					continue
				}
				partialSlabData = append(partialSlabData, data...)
			}
			StaticSatellite.SaveMetadata(ctx, FileMetadata{
				Key:      resp.Object.Key,
				Bucket:   bucket.Name,
				Path:     entry.Name,
				ETag:     resp.Object.ETag,
				MimeType: resp.Object.MimeType,
				Slabs:    resp.Object.Slabs,
				Data:     partialSlabData,
			}, false)
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

	var encrypted string
	parts, exists := s.store.getObject(fmr.Metadata.Bucket, strings.TrimPrefix(fmr.Metadata.Path, "/"))
	if fmr.New {
		if exists {
			err := s.store.deleteObject(fmr.Metadata.Bucket, strings.TrimPrefix(fmr.Metadata.Path, "/"))
			if jc.Check("couldn't delete old object information", err) != nil {
				return
			}
		}
		if cfg.Encrypt {
			if len(fmr.Metadata.Parts) > 0 {
				for i, part := range fmr.Metadata.Parts {
					if i > 0 {
						encrypted += ","
					}
					encrypted += fmt.Sprintf("%d", part)
				}
				err := s.store.addObject(fmr.Metadata.Bucket, strings.TrimPrefix(fmr.Metadata.Path, "/"), fmr.Metadata.Parts)
				if jc.Check("couldn't save object information", err) != nil {
					return
				}
			} else {
				var length uint64
				for _, slab := range fmr.Metadata.Slabs {
					length += uint64(slab.Length)
				}
				encrypted = fmt.Sprintf("%d", length)
				err := s.store.addObject(fmr.Metadata.Bucket, strings.TrimPrefix(fmr.Metadata.Path, "/"), []uint64{length})
				if jc.Check("couldn't save object information", err) != nil {
					return
				}
			}
		}
	} else if exists {
		for i := range parts {
			if i > 0 {
				encrypted += ","
			}
			encrypted += fmt.Sprintf("%d", parts[i])
		}
	}

	encryptedBucket := []byte(fmr.Metadata.Bucket)
	encryptedPath := []byte(fmr.Metadata.Path)
	encryptedMimeType := []byte(fmr.Metadata.MimeType)
	var err error
	if cfg.Encrypt {
		encryptedBucket, err = encodeString(cfg.EncryptionKey, fmr.Metadata.Bucket)
		if jc.Check("couldn't encode bucket", err) != nil {
			return
		}
		encryptedPath, err = encodeString(cfg.EncryptionKey, fmr.Metadata.Path)
		if jc.Check("couldn't encode path", err) != nil {
			return
		}
		encryptedMimeType, err = encodeString(cfg.EncryptionKey, fmr.Metadata.MimeType)
		if jc.Check("couldn't encode MIME type", err) != nil {
			return
		}
	}

	smr := saveMetadataRequest{
		PubKey: pk,
		Metadata: encodedFileMetadata{
			Key:       fmr.Metadata.Key,
			Bucket:    encryptedBucket,
			Path:      encryptedPath,
			ETag:      fmr.Metadata.ETag,
			MimeType:  encryptedMimeType,
			Encrypted: encrypted,
			Slabs:     fmr.Metadata.Slabs,
			Data:      fmr.Metadata.Data,
		},
	}

	h := types.NewHasher()
	smr.EncodeToWithoutSignature(h.E)
	smr.Signature = sk.SignHash(h.Sum())

	host, _, err := net.SplitHostPort(cfg.Address)
	if jc.Check("couldn't get satellite address", err) != nil {
		return
	}
	addr := net.JoinHostPort(host, cfg.MuxPort)

	err = withTransportV3(ctx, cfg.PublicKey, addr, func(t *rhpv3.Transport) (err error) {
		stream := t.DialStream()
		stream.SetDeadline(time.Now().Add(30 * time.Second))

		err = stream.WriteRequest(specifierSaveMetadata, &smr)
		if err != nil {
			return
		}

		dataLen := 1048576
		var ud uploadData
		var resp uploadResponse
		for len(smr.Metadata.Data) > 0 {
			stream.SetDeadline(time.Now().Add(30 * time.Second))
			if len(smr.Metadata.Data) > dataLen {
				ud.Data = smr.Metadata.Data[:dataLen]
			} else {
				ud.Data = smr.Metadata.Data
			}
			smr.Metadata.Data = smr.Metadata.Data[len(ud.Data):]
			ud.More = len(smr.Metadata.Data) > 0
			if err := stream.WriteResponse(&ud); err != nil {
				return err
			}
			if err := stream.ReadResponse(&resp, 1024); err != nil {
				return err
			}
			if resp.DataSize != uint64(len(ud.Data)) {
				return errors.New("wrong data size received")
			}
		}

		return nil
	})

	if jc.Check("couldn't save metadata", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't save file metadata: %s", err))
	}
}

// parseParts is a helper function that converts a comma-separated
// number string to a number slice.
func parseParts(s string) (parts []uint64) {
	for len(s) > 0 {
		i := strings.Index(s, ",")
		if i < 0 {
			i = len(s)
		}
		num, err := strconv.ParseUint(s[:i], 10, 64)
		if err != nil {
			return nil
		}
		parts = append(parts, num)
		if len(s) > i+1 {
			s = s[i+1:]
		} else {
			s = ""
		}
	}
	return
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

	gp, err := s.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)
	rmr := requestMetadataRequest{
		PubKey: pk,
	}

	buckets, err := s.bus.ListBuckets(ctx)
	if jc.Check("couldn't get buckets", err) != nil {
		return
	}

	objs := make(map[string][][]byte)
	for _, bucket := range buckets {
		resp, err := s.bus.ListObjects(ctx, bucket.Name, api.ListObjectOptions{
			Limit: -1,
		})
		if jc.Check("couldn't requests present objects", err) != nil {
			return
		}

		encryptedBucket, err := encodeString(cfg.EncryptionKey, bucket.Name)
		if jc.Check("couldn't encode bucket", err) != nil {
			return
		}
		for _, entry := range resp.Objects {
			_, found := s.store.getObject(bucket.Name, strings.TrimPrefix(entry.Name, "/"))
			if found {
				encryptedPath, err := encodeString(cfg.EncryptionKey, entry.Name)
				if jc.Check("couldn't encode path", err) != nil {
					return
				}
				b := objs[string(encryptedBucket)]
				b = append(b, encryptedPath)
				objs[string(encryptedBucket)] = b
			} else {
				b := objs[bucket.Name]
				b = append(b, []byte(entry.Name))
				objs[bucket.Name] = b
			}
		}
	}

	for b, files := range objs {
		rmr.PresentObjects = append(rmr.PresentObjects, encodedBucketFiles{
			Name:  []byte(b),
			Paths: files,
		})
	}

	h := types.NewHasher()
	rmr.EncodeToWithoutSignature(h.E)
	rmr.Signature = sk.SignHash(h.Sum())

	host, _, err := net.SplitHostPort(cfg.Address)
	if jc.Check("couldn't get satellite address", err) != nil {
		return
	}
	addr := net.JoinHostPort(host, cfg.MuxPort)

	var metadata []encodedFileMetadata
	err = withTransportV3(ctx, cfg.PublicKey, addr, func(t *rhpv3.Transport) (err error) {
		stream := t.DialStream()
		stream.SetDeadline(time.Now().Add(5 * time.Second))
		if err := stream.WriteRequest(specifierRequestMetadata, &rmr); err != nil {
			return err
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			stream.SetDeadline(time.Now().Add(30 * time.Second))
			var rf renterFiles
			if err := stream.ReadResponse(&rf, 1048576); err != nil {
				return err
			}

			start := len(metadata)
			metadata = append(metadata, rf.metadata...)
			for i := range rf.metadata {
				var resp uploadResponse
				if jc.Check("couldn't read response", stream.ReadResponse(&resp, 1024)) != nil {
					return err
				}

				if resp.DataSize == 0 {
					continue
				}

				ud := uploadData{
					More: true,
				}
				maxLen := uint64(1048576) + 8 + 1
				offset := 0
				for ud.More {
					if jc.Check("couldn't read data", stream.ReadResponse(&ud, maxLen)) != nil {
						return err
					}
					copy(metadata[start+i].Data[offset:], ud.Data)
					offset += len(ud.Data)
					resp.DataSize = uint64(len(ud.Data))
					if jc.Check("couldn't write response", stream.WriteResponse(&resp)) != nil {
						return err
					}
				}
			}

			if !rf.more {
				break
			}
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
	for _, fm := range metadata {
		obj := object.Object{
			Key: fm.Key,
		}
		h2c := make(map[types.PublicKey]types.FileContractID)
		for _, c := range contracts {
			h2c[c.HostKey] = c.ID
		}
		used := make(map[types.PublicKey]types.FileContractID)
		for _, s := range fm.Slabs {
			for _, ss := range s.Shards {
				used[ss.LatestHost] = h2c[ss.LatestHost]
			}
		}
		bucket := string(fm.Bucket)
		path := string(fm.Path)
		mimeType := string(fm.MimeType)
		parts := parseParts(fm.Encrypted)
		if len(parts) > 0 {
			bucket, err = decodeString(cfg.EncryptionKey, fm.Bucket)
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't decode bucket: %s", err))
				continue
			}
			path, err = decodeString(cfg.EncryptionKey, fm.Path)
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't decode path: %s", err))
				continue
			}
			mimeType, err = decodeString(cfg.EncryptionKey, fm.MimeType)
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't decode MIME type: %s", err))
				continue
			}
			err = s.store.addObject(bucket, strings.TrimPrefix(path, "/"), parts)
			if jc.Check("couldn't save object information", err) != nil {
				return
			}
		}

		// Check if the object is a directory.
		if strings.HasSuffix(path, "/") {
			continue
		}

		_, err = s.bus.Bucket(ctx, bucket)
		if err != nil {
			err = s.bus.CreateBucket(ctx, bucket, api.CreateBucketOptions{})
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't create bucket: %s", err))
				continue
			}
		}
		_, err = s.bus.Object(ctx, bucket, path, api.GetObjectOptions{})
		if err == nil {
			err = s.bus.DeleteObject(ctx, bucket, path, api.DeleteObjectOptions{})
			if err != nil {
				s.logger.Error(fmt.Sprintf("couldn't delete object: %s", err))
				continue
			}
		}
		var ms, ts int
		if len(fm.Data) > 0 {
			// Deduct redundancy params from the first slab. If there are
			// no complete slabs, use the current redundancy settings.
			ms = gp.RedundancySettings.MinShards
			ts = gp.RedundancySettings.TotalShards
			if len(fm.Slabs) > 0 {
				ms = int(fm.Slabs[0].MinShards)
				ts = len(fm.Slabs[0].Shards)
			}
		}
		for _, slab := range fm.Slabs {
			if slab.IsPartial() {
				ps, _, err := s.bus.AddPartialSlab(ctx, fm.Data[:slab.Length], uint8(ms), uint8(ts), set)
				if err != nil {
					s.logger.Error(fmt.Sprintf("couldn't add partial slab: %s", err))
					continue
				}
				obj.Slabs = append(obj.Slabs, ps...)
				fm.Data = fm.Data[slab.Length:]
			} else {
				for i, shard := range slab.Shards {
					shard.Contracts = map[types.PublicKey][]types.FileContractID{
						shard.LatestHost: {
							used[shard.LatestHost],
						},
					}
					slab.Shards[i] = shard
				}
				obj.Slabs = append(obj.Slabs, slab)
			}
		}
		if err := s.bus.AddObject(ctx, bucket, path, set, obj, api.AddObjectOptions{
			ETag:     fm.ETag,
			MimeType: mimeType,
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
		Slab:   req.Slab,
		Packed: req.Packed,
	}

	h := types.NewHasher()
	usr.EncodeToWithoutSignature(h.E)
	usr.Signature = sk.SignHash(h.Sum())

	err := withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		err = t.WriteRequest(specifierUpdateSlab, &usr)
		if err != nil {
			return
		}

		var resp rhpv2.RPCError
		err = t.ReadResponse(&resp, 1024)
		if err != nil {
			return
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})
	if err != nil {
		s.logger.Error(fmt.Sprintf("couldn't update slab: %s", err))
	}
}

// requestSlabsHandler handles the GET /slabs requests.
func (s *Satellite) requestSlabsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't request modified slabs: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	set := jc.PathParam("set")
	if set == "" {
		jc.Check("ERROR", errors.New("contract set cannot be empty"))
		return
	}
	s.logger.Info("requesting modified slabs from the satellite")
	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(cfg.RenterSeed)
	rsr := requestSlabsRequest{
		PubKey: pk,
	}

	h := types.NewHasher()
	rsr.EncodeToWithoutSignature(h.E)
	rsr.Signature = sk.SignHash(h.Sum())

	var ms modifiedSlabs
	err := withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRequestSlabs, &rsr); err != nil {
			return err
		}

		if err := t.ReadResponse(&ms, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't request modified slabs", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't request modified slabs: %s", err))
		return
	}

	contracts, err := s.bus.ContractSetContracts(ctx, set)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range contracts {
		h2c[c.HostKey] = c.ID
	}

	var numSlabs int
	for _, slab := range ms.slabs {
		for i, shard := range slab.Shards {
			shard.Contracts = map[types.PublicKey][]types.FileContractID{
				shard.LatestHost: {
					h2c[shard.LatestHost],
				},
			}
			slab.Shards[i] = shard
		}
		if err := s.bus.UpdateSlab(ctx, slab, set); err != nil {
			s.logger.Error(fmt.Sprintf("couldn't update slab: %s", err))
			continue
		}
		numSlabs++
	}

	s.logger.Info(fmt.Sprintf("successfully updated %d slabs", numSlabs))
	jc.Encode(ms.slabs)
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

	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
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

func newMimeReader(r io.Reader) (mimeType string, recycled io.Reader, err error) {
	buf := bytes.NewBuffer(nil)
	mtype, err := mimetype.DetectReader(io.TeeReader(r, buf))
	recycled = io.MultiReader(buf, r)
	return mtype.String(), recycled, err
}

// UploadObject uploads a file to the satellite.
func UploadObject(r io.Reader, bucket, path, mimeType string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := StaticSatellite.Config()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return errors.New("couldn't upload object: satellite disabled")
	}

	// if not given, try decide on a mime type using the file extension
	if mimeType == "" {
		mimeType = mime.TypeByExtension(filepath.Ext(path))

		// if mime type is still not known, wrap the reader with a mime reader
		if mimeType == "" {
			var err error
			mimeType, r, err = newMimeReader(r)
			if err != nil {
				return err
			}
		}
	}

	if cfg.Encrypt {
		r, err = encrypt.Encrypt(r, cfg.EncryptionKey)
		if err != nil {
			return err
		}
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	encryptedBucket := []byte(bucket)
	encryptedPath := []byte(path)
	encryptedMimeType := []byte(mimeType)
	if cfg.Encrypt {
		encryptedBucket, err = encodeString(cfg.EncryptionKey, bucket)
		if err != nil {
			return err
		}
		encryptedPath, err = encodeString(cfg.EncryptionKey, path)
		if err != nil {
			return err
		}
		encryptedMimeType, err = encodeString(cfg.EncryptionKey, mimeType)
		if err != nil {
			return err
		}
	}

	req := uploadRequest{
		PubKey:    pk,
		Bucket:    encryptedBucket,
		Path:      encryptedPath,
		MimeType:  encryptedMimeType,
		Encrypted: cfg.Encrypt,
	}
	h := types.NewHasher()
	req.EncodeToWithoutSignature(h.E)
	req.Signature = sk.SignHash(h.Sum())

	host, _, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(host, cfg.MuxPort)

	err = withTransportV3(ctx, cfg.PublicKey, addr, func(t *rhpv3.Transport) (err error) {
		stream := t.DialStream()
		stream.SetDeadline(time.Now().Add(30 * time.Second))
		err = stream.WriteRequest(specifierUploadFile, &req)
		if err != nil {
			return err
		}

		var resp uploadResponse
		err = stream.ReadResponse(&resp, 1024)
		if err != nil {
			return err
		}

		dataLen := uint64(1048576)
		buf := make([]byte, dataLen)
		var ud uploadData
		var total uint64
		incompleteChunk := resp.DataSize % dataLen
		completeChunks := resp.DataSize - incompleteChunk
		for total < completeChunks {
			_, err := io.ReadFull(r, buf)
			if err != nil {
				return stream.WriteResponse(&ud)
			}
			total += dataLen
		}
		if incompleteChunk > 0 {
			buf := make([]byte, incompleteChunk)
			_, err := io.ReadFull(r, buf)
			if err != nil {
				return stream.WriteResponse(&ud)
			}
		}

		for {
			stream.SetDeadline(time.Now().Add(30 * time.Second))
			numBytes, err := io.ReadFull(r, buf)
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}
			ud.Data = buf[:numBytes]
			ud.More = err == nil
			if err := stream.WriteResponse(&ud); err != nil {
				return err
			}
			if err := stream.ReadResponse(&resp, 1024); err != nil {
				return err
			}
			if !ud.More {
				break
			}
		}

		return nil
	})

	return err
}

// encodeString encrypts a string with the encryption key.
func encodeString(key object.EncryptionKey, str string) (ciphertext []byte, err error) {
	rs, err := encrypt.Encrypt(bytes.NewReader([]byte(str)), key)
	if err != nil {
		return
	}

	ciphertext, err = io.ReadAll(rs)
	return
}

// decodeString decrypts a string encrypted with the encryption key.
func decodeString(key object.EncryptionKey, ciphertext []byte) (string, error) {
	var out bytes.Buffer
	ws, err := encrypt.Decrypt(&out, key, nil)
	if err != nil {
		return "", err
	}

	_, err = ws.Write(ciphertext)
	if err != nil {
		return "", err
	}

	return out.String(), nil
}

// createMultipartHandler handles the POST /multipart/create requests.
func (s *Satellite) createMultipartHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't register multipart upload: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var req CreateMultipartRequest
	if jc.Decode(&req) != nil {
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	encryptedBucket := []byte(req.Bucket)
	encryptedPath := []byte(req.Path)
	encryptedMimeType := []byte(req.MimeType)
	var err error
	if cfg.Encrypt {
		encryptedBucket, err = encodeString(cfg.EncryptionKey, req.Bucket)
		if jc.Check("couldn't encode bucket", err) != nil {
			return
		}
		encryptedPath, err = encodeString(cfg.EncryptionKey, req.Path)
		if jc.Check("couldn't encode path", err) != nil {
			return
		}
		encryptedMimeType, err = encodeString(cfg.EncryptionKey, req.MimeType)
		if jc.Check("couldn't encode MIME type", err) != nil {
			return
		}
	}

	rmr := registerMultipartRequest{
		PubKey:    pk,
		Key:       req.Key,
		Bucket:    encryptedBucket,
		Path:      encryptedPath,
		MimeType:  encryptedMimeType,
		Encrypted: cfg.Encrypt,
	}

	h := types.NewHasher()
	rmr.EncodeToWithoutSignature(h.E)
	rmr.Signature = sk.SignHash(h.Sum())

	var resp registerMultipartResponse
	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierCreateMultipart, &rmr); err != nil {
			return err
		}

		if err := t.ReadResponse(&resp, 1024); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't register multipart upload", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't register multipart upload: %s", err))
		return
	}

	response := CreateMultipartResponse{
		UploadID: hex.EncodeToString(resp.UploadID[:]),
	}

	s.logger.Debug(fmt.Sprintf("successfully registered multipart upload %s", response.UploadID))
	jc.Encode(response)
}

// abortMultipartHandler handles the POST /multipart/abort requests.
func (s *Satellite) abortMultipartHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't delete multipart upload: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var req CreateMultipartResponse
	if jc.Decode(&req) != nil {
		return
	}
	id, err := hex.DecodeString(req.UploadID)
	if jc.Check("couldn't marshal upload ID", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't marshal upload ID: %s", err))
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	dmr := deleteMultipartRequest{
		PubKey: pk,
	}
	copy(dmr.UploadID[:], id)

	h := types.NewHasher()
	dmr.EncodeToWithoutSignature(h.E)
	dmr.Signature = sk.SignHash(h.Sum())

	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierAbortMultipart, &dmr); err != nil {
			return err
		}

		var resp rhpv2.RPCError
		if err := t.ReadResponse(&resp, 1024); err != nil {
			return err
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})

	if jc.Check("couldn't delete multipart upload", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't delete multipart upload: %s", err))
		return
	}

	s.logger.Debug(fmt.Sprintf("multipart upload %s aborted", req.UploadID))
}

// UploadPart uploads a part of an S3 multipart upload to the satellite.
func UploadPart(r io.Reader, id string, part int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := StaticSatellite.Config()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		return errors.New("couldn't upload part: satellite disabled")
	}

	if cfg.Encrypt {
		r, err = encrypt.Encrypt(r, cfg.EncryptionKey)
		if err != nil {
			return err
		}
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	uid, err := hex.DecodeString(id)
	if err != nil {
		return err
	}

	req := uploadPartRequest{
		PubKey:     pk,
		PartNumber: part,
	}
	copy(req.UploadID[:], uid)
	h := types.NewHasher()
	req.EncodeToWithoutSignature(h.E)
	req.Signature = sk.SignHash(h.Sum())

	host, _, err := net.SplitHostPort(cfg.Address)
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(host, cfg.MuxPort)

	err = withTransportV3(ctx, cfg.PublicKey, addr, func(t *rhpv3.Transport) (err error) {
		stream := t.DialStream()
		stream.SetDeadline(time.Now().Add(5 * time.Second))
		err = stream.WriteRequest(specifierUploadPart, &req)
		if err != nil {
			return err
		}

		dataLen := uint64(1048576)
		buf := make([]byte, dataLen)
		var resp uploadResponse
		var ud uploadData

		for {
			stream.SetDeadline(time.Now().Add(30 * time.Second))
			numBytes, err := io.ReadFull(r, buf)
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				return err
			}
			ud.Data = buf[:numBytes]
			ud.More = err == nil
			if err := stream.WriteResponse(&ud); err != nil {
				return err
			}
			if err := stream.ReadResponse(&resp, 1024); err != nil {
				return err
			}
			if !ud.More {
				break
			}
		}

		return nil
	})

	return err
}

// completeMultipartHandler handles the POST /multipart/complete requests.
func (s *Satellite) completeMultipartHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		s.logger.Error("couldn't complete multipart upload: satellite disabled")
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var req CreateMultipartResponse
	if jc.Decode(&req) != nil {
		return
	}
	id, err := hex.DecodeString(req.UploadID)
	if jc.Check("couldn't marshal upload ID", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't marshal upload ID: %s", err))
		return
	}

	pk, sk := generateKeyPair(cfg.RenterSeed)

	cmr := completeMultipartRequest{
		PubKey: pk,
	}
	copy(cmr.UploadID[:], id)

	h := types.NewHasher()
	cmr.EncodeToWithoutSignature(h.E)
	cmr.Signature = sk.SignHash(h.Sum())

	err = withTransportV2(ctx, cfg.PublicKey, cfg.Address, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierCompleteMultipart, &cmr); err != nil {
			return err
		}

		var resp rhpv2.RPCError
		if err := t.ReadResponse(&resp, 1024); err != nil {
			return err
		}

		if resp.Description != "" {
			return errors.New(resp.Description)
		}

		return nil
	})

	if jc.Check("couldn't complete multipart upload", err) != nil {
		s.logger.Error(fmt.Sprintf("couldn't complete multipart upload: %s", err))
		return
	}

	s.logger.Debug(fmt.Sprintf("multipart upload %s completed", req.UploadID))
}
