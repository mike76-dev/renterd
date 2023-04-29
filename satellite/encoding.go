package satellite

import (
	"go.sia.tech/core/types"
)

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
	e.WriteBytes(fr.SecretKey[:])
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
	e.WriteUint64(fr.BlockHeightLeeway)
	fr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (fr *formRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(fr.PubKey[:])
	e.WriteBytes(fr.SecretKey[:])
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
	e.WriteUint64(fr.BlockHeightLeeway)
}

// DecodeFrom implements types.ProtocolObject.
func (fr *formRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (rr *renewRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
	e.WriteBytes(rr.SecretKey[:])
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
	e.WriteUint64(rr.BlockHeightLeeway)
	rr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rr *renewRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(rr.PubKey[:])
	e.WriteBytes(rr.SecretKey[:])
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
	e.WriteUint64(rr.BlockHeightLeeway)
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

// EncodeTo implements types.ProtocolObject.
func (ec *extendedContract) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (ec *extendedContract) DecodeFrom(d *types.Decoder) {
	ec.contract.Revision.DecodeFrom(d)
	ec.contract.Signatures[0].DecodeFrom(d)
	ec.contract.Signatures[1].DecodeFrom(d)
	ec.startHeight = d.ReadUint64()
	ec.totalCost.DecodeFrom(d)
	ec.uploadSpending.DecodeFrom(d)
	ec.downloadSpending.DecodeFrom(d)
	ec.fundAccountSpending.DecodeFrom(d)
	ec.renewedFrom.DecodeFrom(d)
}

// EncodeTo implements types.ProtocolObject.
func (ecs *extendedContractSet) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (ecs *extendedContractSet) DecodeFrom(d *types.Decoder) {
	num := d.ReadUint64()
	ecs.contracts = make([]extendedContract, 0, num)
	for num > 0 {
		var ec extendedContract
		ec.DecodeFrom(d)
		ecs.contracts = append(ecs.contracts, ec)
		num--
	}
}

// EncodeTo implements types.ProtocolObject.
func (fcr *formContractRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(fcr.PubKey[:])
	e.WriteBytes(fcr.RenterKey[:])
	e.WriteBytes(fcr.HostKey[:])
	e.WriteUint64(fcr.EndHeight)
	e.WriteUint64(fcr.Storage)
	e.WriteUint64(fcr.Upload)
	e.WriteUint64(fcr.Download)
	e.WriteUint64(fcr.MinShards)
	e.WriteUint64(fcr.TotalShards)
	fcr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (fcr *formContractRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(fcr.PubKey[:])
	e.WriteBytes(fcr.RenterKey[:])
	e.WriteBytes(fcr.HostKey[:])
	e.WriteUint64(fcr.EndHeight)
	e.WriteUint64(fcr.Storage)
	e.WriteUint64(fcr.Upload)
	e.WriteUint64(fcr.Download)
	e.WriteUint64(fcr.MinShards)
	e.WriteUint64(fcr.TotalShards)
}

// DecodeFrom implements types.ProtocolObject.
func (fcr *formContractRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (rcr *renewContractRequest) EncodeTo(e *types.Encoder) {
	e.WriteBytes(rcr.PubKey[:])
	e.WriteBytes(rcr.Contract[:])
	e.WriteUint64(rcr.EndHeight)
	e.WriteUint64(rcr.Storage)
	e.WriteUint64(rcr.Upload)
	e.WriteUint64(rcr.Download)
	e.WriteUint64(rcr.MinShards)
	e.WriteUint64(rcr.TotalShards)
	rcr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rcr *renewContractRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.WriteBytes(rcr.PubKey[:])
	e.WriteBytes(rcr.Contract[:])
	e.WriteUint64(rcr.EndHeight)
	e.WriteUint64(rcr.Storage)
	e.WriteUint64(rcr.Upload)
	e.WriteUint64(rcr.Download)
	e.WriteUint64(rcr.MinShards)
	e.WriteUint64(rcr.TotalShards)
}

// DecodeFrom implements types.ProtocolObject.
func (rcr *renewContractRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (rh *revisionHash) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (rh *revisionHash) DecodeFrom(d *types.Decoder) {
	rh.RevisionHash.DecodeFrom(d)
}

// EncodeTo implements types.ProtocolObject.
func (rs *renterSignature) EncodeTo(e *types.Encoder) {
	rs.Signature.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (rs *renterSignature) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}
