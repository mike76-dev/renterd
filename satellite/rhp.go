package satellite

import (
	"errors"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
)

var (
	specifierRequestContracts = types.NewSpecifier("RequestContracts")
	specifierFormContracts    = types.NewSpecifier("FormContracts")
	specifierRenewContracts   = types.NewSpecifier("RenewContracts")
	specifierUpdateRevision   = types.NewSpecifier("UpdateRevision")
)

// requestRequest is used to request existing contracts.
type requestRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
}

// formRequest is used to request contract formation.
type formRequest struct {
	PubKey      types.PublicKey
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

// renewRequest is used to request contract renewal.
type renewRequest struct {
	PubKey      types.PublicKey
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

// generateKeyPair generates the keypair from a given seed.
func generateKeyPair(seed []byte) (types.PublicKey, types.PrivateKey) {
	privKey := types.NewPrivateKeyFromSeed(seed)
	return privKey.PublicKey(), privKey
}

// requestContractsHandler handles the /request requests.
func (s *Satellite) requestContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
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
		return
	}
	
	var added []api.ContractMetadata
	var contracts []types.FileContractID

	existing, _ := s.bus.Contracts(ctx, "autopilot")
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	var recs []api.ContractSpendingRecord
	for _, ec := range ecs.contracts {
		id := ec.contract.ID()
		contracts = append(contracts, id)
		_, err = s.bus.Contract(ctx, id)
		if err == nil {
			continue
		}
		var a api.ContractMetadata
		if (ec.renewedFrom == types.FileContractID{}) {
			a, err = s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight, cfg.PublicKey)
		} else {
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.totalCost, ec.startHeight, ec.renewedFrom, cfg.PublicKey)
		}
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
		recs = append(recs, api.ContractSpendingRecord{
			ContractSpending: api.ContractSpending{
				Uploads:     ec.uploadSpending,
				Downloads:   ec.downloadSpending,
				FundAccount: ec.fundAccountSpending,
			},
			ContractID:  id,
		})
	}
	err = s.bus.RecordContractSpending(ctx, recs)
	if jc.Check("couldn't update contract spendings", err) != nil {
		return
	}

	err = s.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		return
	}

	jc.Encode(added)
}

// formContractsHandler handles the /form requests.
func (s *Satellite) formContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
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
		return
	}
	
	var added []api.ContractMetadata
	var contracts []types.FileContractID

	existing, _ := s.bus.Contracts(ctx, "autopilot")
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	for _, ec := range ecs.contracts {
		id := ec.contract.ID()
		contracts = append(contracts, id)
		a, err := s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight, cfg.PublicKey)
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
	}
	err = s.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		return
	}

	jc.Encode(added)
}

// renewContractsHandler handles the /renew requests.
func (s *Satellite) renewContractsHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
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
		return
	}

	var added []api.ContractMetadata

	for _, ec := range ecs.contracts {
		host := ec.contract.HostKey()
		from, ok := renewedFrom[host]
		var a api.ContractMetadata
		if ok {
			a, err = s.bus.AddRenewedContract(ctx, ec.contract, ec.totalCost, ec.startHeight, from, cfg.PublicKey)
		} else {
			a, err = s.bus.AddContract(ctx, ec.contract, ec.totalCost, ec.startHeight, cfg.PublicKey)
		}
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
	}

	jc.Encode(added)
}

// updateRevisionHandler submits an updated contract revision to the satellite.
func (s *Satellite) updateRevisionHandler(jc jape.Context) {
	cfg := s.store.getConfig()
	if !cfg.Enabled {
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var sur UpdateRevisionRequest
	if jc.Decode(&sur) != nil {
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
	if jc.Check("could not connect to the host", err) != nil {
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
