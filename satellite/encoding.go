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
		ec.totalCost.DecodeFrom(d)
		ec.uploadSpending.DecodeFrom(d)
		ec.downloadSpending.DecodeFrom(d)
		ec.fundAccountSpending.DecodeFrom(d)
		ec.renewedFrom.DecodeFrom(d)
		ecs.contracts = append(ecs.contracts, ec)
		num--
	}
}
