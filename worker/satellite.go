package worker

import (
	"context"
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

// EncodeTo implements types.ProtocolObject.
func (rr *requestRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
	rr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rr *requestRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
}

// DecodeFrom implements types.ProtocolObject.
func (rr *requestRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (fr *formRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(fr.PubKey[:])
	e.WriteUint64(fr.Hosts)
	e.WriteUint64(fr.Period)
	e.WriteUint64(fr.RenewWindow)
	e.WriteUint64(fr.Storage)
	e.WriteUint64(fr.Upload)
	e.WriteUint64(fr.Download)
	e.WriteUint64(fr.MinShards)
	e.WriteUint64(fr.TotalShards)
	fr.MaxRPCPrice.EncodeTo(e)
	fr.MaxContractPrice.EncodeTo(e)
	fr.MaxDownloadPrice.EncodeTo(e)
	fr.MaxUploadPrice.EncodeTo(e)
	fr.MaxStoragePrice.EncodeTo(e)
	fr.MaxSectorAccessPrice.EncodeTo(e)
	fr.MinMaxCollateral.EncodeTo(e)
	fr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (fr *formRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(fr.PubKey[:])
	e.WriteUint64(fr.Hosts)
	e.WriteUint64(fr.Period)
	e.WriteUint64(fr.RenewWindow)
	e.WriteUint64(fr.Storage)
	e.WriteUint64(fr.Upload)
	e.WriteUint64(fr.Download)
	e.WriteUint64(fr.MinShards)
	e.WriteUint64(fr.TotalShards)
	fr.MaxRPCPrice.EncodeTo(e)
	fr.MaxContractPrice.EncodeTo(e)
	fr.MaxDownloadPrice.EncodeTo(e)
	fr.MaxUploadPrice.EncodeTo(e)
	fr.MaxStoragePrice.EncodeTo(e)
	fr.MaxSectorAccessPrice.EncodeTo(e)
	fr.MinMaxCollateral.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (fr *formRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (rr *renewRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
	e.WriteUint64(uint64(len(rr.Contracts)))
	for _, c := range rr.Contracts {
		e.WriteBytes(c[:])
	}
	e.WriteUint64(rr.Period)
	e.WriteUint64(rr.RenewWindow)
	e.WriteUint64(rr.Storage)
	e.WriteUint64(rr.Upload)
	e.WriteUint64(rr.Download)
	e.WriteUint64(rr.MinShards)
	e.WriteUint64(rr.TotalShards)
	rr.MaxRPCPrice.EncodeTo(e)
	rr.MaxContractPrice.EncodeTo(e)
	rr.MaxDownloadPrice.EncodeTo(e)
	rr.MaxUploadPrice.EncodeTo(e)
	rr.MaxStoragePrice.EncodeTo(e)
	rr.MaxSectorAccessPrice.EncodeTo(e)
	rr.MinMaxCollateral.EncodeTo(e)
	rr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rr *renewRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
	e.WriteUint64(uint64(len(rr.Contracts)))
	for _, c := range rr.Contracts {
		e.WriteBytes(c[:])
	}
	e.WriteUint64(rr.Period)
	e.WriteUint64(rr.RenewWindow)
	e.WriteUint64(rr.Storage)
	e.WriteUint64(rr.Upload)
	e.WriteUint64(rr.Download)
	e.WriteUint64(rr.MinShards)
	e.WriteUint64(rr.TotalShards)
	rr.MaxRPCPrice.EncodeTo(e)
	rr.MaxContractPrice.EncodeTo(e)
	rr.MaxDownloadPrice.EncodeTo(e)
	rr.MaxUploadPrice.EncodeTo(e)
	rr.MaxStoragePrice.EncodeTo(e)
	rr.MaxSectorAccessPrice.EncodeTo(e)
	rr.MinMaxCollateral.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (rr *renewRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (ur *updateRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(ur.PubKey[:])
	ur.Contract.Revision.EncodeTo(e)
	ur.Contract.Signatures[0].EncodeTo(e)
	ur.Contract.Signatures[1].EncodeTo(e)
	ur.Uploads.EncodeTo(e)
	ur.Downloads.EncodeTo(e)
	ur.FundAccount.EncodeTo(e)
	ur.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (ur *updateRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(ur.PubKey[:])
	ur.Contract.Revision.EncodeTo(e)
	ur.Contract.Signatures[0].EncodeTo(e)
	ur.Contract.Signatures[1].EncodeTo(e)
	ur.Uploads.EncodeTo(e)
	ur.Downloads.EncodeTo(e)
	ur.FundAccount.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (ur *updateRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// contractSet is a collection of rhpv2.ContractRevision
// objects.
type contractSet struct {
	contracts []rhpv2.ContractRevision
}

// EncodeTo implements types.ProtocolObject.
func (cs *contractSet) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (cs *contractSet) DecodeFrom(d *types.Decoder) {
	num := d.ReadUint64()
	cs.contracts = make([]rhpv2.ContractRevision, 0, num)
	var cr rhpv2.ContractRevision
	for num > 0 {
		cr.Revision.DecodeFrom(d)
		cr.Signatures[0].DecodeFrom(d)
		cr.Signatures[1].DecodeFrom(d)
		cs.contracts = append(cs.contracts, cr)
		num--
	}
}

// extendedContract contains additionally the block height the contract
// was created at.
type extendedContract struct {
	contract    rhpv2.ContractRevision
	startHeight uint64
}

// extendedContractSet is a collection of extendedContracts.
type extendedContractSet struct {
	contracts []extendedContract
}

// EncodeTo implements types.ProtocolObject.
func (ecs *extendedContractSet) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (ecs *extendedContractSet) DecodeFrom(d *types.Decoder) {
	num := d.ReadUint64()
	ecs.contracts = make([]extendedContract, 0, num)
	var ec extendedContract
	for num > 0 {
		ec.contract.Revision.DecodeFrom(d)
		ec.contract.Signatures[0].DecodeFrom(d)
		ec.contract.Signatures[1].DecodeFrom(d)
		ec.startHeight = d.ReadUint64()
		ecs.contracts = append(ecs.contracts, ec)
		num--
	}
}

// rpcMessage represents a simple RPC response.
type rpcMessage struct {
	Error string
}

// EncodeTo implements types.ProtocolObject.
func (r *rpcMessage) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (r *rpcMessage) DecodeFrom(d *types.Decoder) {
	r.Error = d.ReadString()
}

// generateKeyPair generates the keypair from a given seed.
func generateKeyPair(seed []byte) (types.PublicKey, types.PrivateKey) {
	privKey := types.NewPrivateKeyFromSeed(seed)
	return privKey.PublicKey(), privKey
}

// satelliteRequestContractsHandler handles the /satellite/request requests.
func (w *worker) satelliteRequestContractsHandler(jc jape.Context) {
	if !w.pool.satelliteEnabled {
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()

	pk, sk := generateKeyPair(w.pool.satelliteRenterSeed)

	rr := requestRequest{
		PubKey: pk,
	}

	h := types.NewHasher()
	rr.EncodeToWithoutSignature(h.E)
	rr.Signature = sk.SignHash(h.Sum())

	var ecs extendedContractSet
	err := w.withTransportV2(ctx, w.pool.satellitePublicKey, w.pool.satelliteAddress, func(t *rhpv2.Transport) (err error) {
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

	existing, _ := w.bus.Contracts(ctx, "autopilot")
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	for _, ec := range ecs.contracts {
		id := ec.contract.ID()
		contracts = append(contracts, id)
		_, err = w.bus.Contract(ctx, id)
		if err == nil {
			continue
		}
		a, err := w.bus.AddContract(ctx, ec.contract, ec.contract.RenterFunds(), ec.startHeight)
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
	}
	err = w.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		return
	}

	jc.Encode(added)
}

// RequestContracts requests the existing active contracts from the
// satellite and adds them to the contract set.
func (c *Client) RequestContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).GET("/satellite/form", &resp)
	return resp, err
}

// satelliteFormContractsHandler handles the /satellite/form requests.
func (w *worker) satelliteFormContractsHandler(jc jape.Context) {
	if !w.pool.satelliteEnabled {
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var sfr api.SatelliteFormRequest
	if jc.Decode(&sfr) != nil {
		return
	}

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(w.pool.satelliteRenterSeed)

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
	}

	h := types.NewHasher()
	fr.EncodeToWithoutSignature(h.E)
	fr.Signature = sk.SignHash(h.Sum())

	state, err := w.bus.ConsensusState(ctx)
	if err != nil {
		jc.Check("ERROR", errors.New("could not get consensus state"))
		return
	}

	var cs contractSet
	err = w.withTransportV2(ctx, w.pool.satellitePublicKey, w.pool.satelliteAddress, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierFormContracts, &fr); err != nil {
			return err
		}

		if err := t.ReadResponse(&cs, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't form contracts", err) != nil {
		return
	}
	
	var added []api.ContractMetadata
	var contracts []types.FileContractID

	existing, _ := w.bus.Contracts(ctx, "autopilot")
	for _, c := range existing {
		contracts = append(contracts, c.ID)
	}

	for _, cr := range cs.contracts {
		id := cr.ID()
		contracts = append(contracts, id)
		a, err := w.bus.AddContract(ctx, cr, cr.RenterFunds(), state.BlockHeight)
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
	}
	err = w.bus.SetContractSet(ctx, "autopilot", contracts)
	if jc.Check("couldn't set contract set", err) != nil {
		return
	}

	jc.Encode(added)
}

// FormContracts requests the satellite to form the specified number
// of contracts with the hosts and adds them to the contract set.
func (c *Client) FormContracts(ctx context.Context, hosts uint64, period uint64, renewWindow uint64, storage uint64, upload uint64, download uint64) ([]api.ContractMetadata, error) {
	req := api.SatelliteFormRequest{
		Hosts:       hosts,
		Period:      period,
		RenewWindow: renewWindow,
		Download:    download,
		Upload:      upload,
		Storage:     storage,
	}
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/satellite/form", req, &resp)
	return resp, err
}

// satelliteRenewContractsHandler handles the /satellite/renew requests.
func (w *worker) satelliteRenewContractsHandler(jc jape.Context) {
	if !w.pool.satelliteEnabled {
		jc.Check("ERROR", errors.New("satellite disabled"))
		return
	}
	ctx := jc.Request.Context()
	var srr api.SatelliteRenewRequest
	if jc.Decode(&srr) != nil {
		return
	}

	renewedFrom := make(map[types.PublicKey]types.FileContractID)
	for _, id := range srr.Contracts {
		contract, err := w.bus.Contract(ctx, id)
		if err != nil {
			continue
		}
		renewedFrom[contract.HostKey] = id
	}

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	pk, sk := generateKeyPair(w.pool.satelliteRenterSeed)

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
	}

	h := types.NewHasher()
	rr.EncodeToWithoutSignature(h.E)
	rr.Signature = sk.SignHash(h.Sum())

	var cs contractSet
	err = w.withTransportV2(ctx, w.pool.satellitePublicKey, w.pool.satelliteAddress, func(t *rhpv2.Transport) (err error) {
		if err := t.WriteRequest(specifierRenewContracts, &rr); err != nil {
			return err
		}

		if err := t.ReadResponse(&cs, 65536); err != nil {
			return err
		}

		return nil
	})

	if jc.Check("couldn't renew contracts", err) != nil {
		return
	}

	var added []api.ContractMetadata

	for _, cr := range cs.contracts {
		state, err := w.bus.ConsensusState(ctx)
		if err != nil {
			jc.Check("ERROR", errors.New("could not get consensus state"))
			return
		}
		host := cr.HostKey()
		from, ok := renewedFrom[host]
		var a api.ContractMetadata
		if ok {
			a, err = w.bus.AddRenewedContract(ctx, cr, cr.RenterFunds(), state.BlockHeight, from)
		} else {
			a, err = w.bus.AddContract(ctx, cr, cr.RenterFunds(), state.BlockHeight)
		}
		if jc.Check("couldn't add contract", err) != nil {
			return
		}
		added = append(added, a)
	}

	jc.Encode(added)
}

// RenewContracts requests the satellite to renew the given set
// of contracts and add them to the contract set.
func (c *Client) RenewContracts(ctx context.Context, contracts []types.FileContractID, period uint64, renewWindow uint64, storage uint64, upload uint64, download uint64) ([]api.ContractMetadata, error) {
	req := api.SatelliteRenewRequest{
		Contracts:   contracts,
		Period:      period,
		RenewWindow: renewWindow,
		Download:    download,
		Upload:      upload,
		Storage:     storage,
	}
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/satellite/renew", req, &resp)
	return resp, err
}

// satelliteUpdateRevision submits an updated contract revision to
// the satellite.
func (s *Session) satelliteUpdateRevision(spending api.ContractSpending) (err error) {
	return satelliteUpdateRevision(s.Revision(), s.satelliteRenterSeed, s.satelliteAddress, s.satellitePublicKey, spending)
}

// satelliteUpdateRevision submits an updated contract revision to
// the satellite.
func (w *worker) satelliteUpdateRevision(rev rhpv2.ContractRevision, spending api.ContractSpending) (err error) {
	return satelliteUpdateRevision(rev, w.pool.satelliteRenterSeed, w.pool.satelliteAddress, w.pool.satellitePublicKey, spending)
}

// satelliteUpdateRevision submits an updated contract revision to
// the satellite.
func satelliteUpdateRevision(rev rhpv2.ContractRevision, seed []byte, addr string, pubKey types.PublicKey, spending api.ContractSpending) (err error) {
	pk, sk := generateKeyPair(seed)

	ur := updateRequest{
		PubKey:      pk,
		Contract:    rev,
		Uploads:     spending.Uploads,
		Downloads:   spending.Downloads,
		FundAccount: spending.FundAccount,
	}

	h := types.NewHasher()
	ur.EncodeToWithoutSignature(h.E)
	ur.Signature = sk.SignHash(h.Sum())

	var resp rpcMessage
	ctx := context.Background()
	conn, err := dial(ctx, addr, pubKey)
	if err != nil {
		return err
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
			err = ctx.Err()
		}
	}()

	t, err := rhpv2.NewRenterTransport(conn, pubKey)
	if err != nil {
		return
	}
	defer t.Close()

	err = t.WriteRequest(specifierUpdateRevision, &ur)
	if err != nil {
		return
	}

	err = t.ReadResponse(&resp, 1024)
	if err != nil {
		return err
	}

	if resp.Error != "" {
		return errors.New(resp.Error)
	}

	return
}
