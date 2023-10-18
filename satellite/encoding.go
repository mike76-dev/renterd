package satellite

import (
	"encoding/hex"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
)

// EncodeTo implements types.ProtocolObject.
func (rr *requestRequest) EncodeTo(e *types.Encoder) {
	e.Write(rr.PubKey[:])
	rr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rr *requestRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rr.PubKey[:])
}

// DecodeFrom implements types.ProtocolObject.
func (rr *requestRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (fr *formRequest) EncodeTo(e *types.Encoder) {
	fr.EncodeToWithoutSignature(e)
	fr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (fr *formRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(fr.PubKey[:])
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
	rr.EncodeToWithoutSignature(e)
	rr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rr *renewRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rr.PubKey[:])
	e.WriteBytes(rr.SecretKey[:])
	e.WriteUint64(uint64(len(rr.Contracts)))
	for _, c := range rr.Contracts {
		e.Write(c[:])
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
	ur.EncodeToWithoutSignature(e)
	ur.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (ur *updateRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(ur.PubKey[:])
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
	fcr.EncodeToWithoutSignature(e)
	fcr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (fcr *formContractRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(fcr.PubKey[:])
	e.Write(fcr.RenterKey[:])
	e.Write(fcr.HostKey[:])
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
	rcr.EncodeToWithoutSignature(e)
	rcr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rcr *renewContractRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rcr.PubKey[:])
	e.Write(rcr.Contract[:])
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
func (gsr *getSettingsRequest) EncodeTo(e *types.Encoder) {
	gsr.EncodeToWithoutSignature(e)
	gsr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (gsr *getSettingsRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(gsr.PubKey[:])
}

// DecodeFrom implements types.ProtocolObject.
func (gsr *getSettingsRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (settings *RenterSettings) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (settings *RenterSettings) DecodeFrom(d *types.Decoder) {
	settings.AutoRenewContracts = d.ReadBool()
	settings.BackupFileMetadata = d.ReadBool()
	settings.AutoRepairFiles = d.ReadBool()
}

// EncodeTo implements types.ProtocolObject.
func (usr *updateSettingsRequest) EncodeTo(e *types.Encoder) {
	usr.EncodeToWithoutSignature(e)
	usr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (usr *updateSettingsRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(usr.PubKey[:])
	e.WriteBool(usr.AutoRenewContracts)
	e.WriteBool(usr.BackupFileMetadata)
	e.WriteBool(usr.AutoRepairFiles)
	if usr.AutoRenewContracts || usr.AutoRepairFiles {
		e.WriteBytes(usr.SecretKey)
	}
	if usr.AutoRepairFiles {
		e.WriteBytes(usr.AccountKey)
	}
	if usr.AutoRenewContracts {
		e.WriteUint64(usr.Hosts)
		e.WriteUint64(usr.Period)
		e.WriteUint64(usr.RenewWindow)
		e.WriteUint64(usr.Storage)
		e.WriteUint64(usr.Upload)
		e.WriteUint64(usr.Download)
		e.WriteUint64(usr.MinShards)
		e.WriteUint64(usr.TotalShards)
		usr.MaxRPCPrice.EncodeTo(e)
		usr.MaxContractPrice.EncodeTo(e)
		usr.MaxDownloadPrice.EncodeTo(e)
		usr.MaxUploadPrice.EncodeTo(e)
		usr.MaxStoragePrice.EncodeTo(e)
		usr.MaxSectorAccessPrice.EncodeTo(e)
		usr.MinMaxCollateral.EncodeTo(e)
		e.WriteUint64(usr.BlockHeightLeeway)
	}
}

// DecodeFrom implements types.ProtocolObject.
func (usr *updateSettingsRequest) DecodeFrom(d *types.Decoder) {
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

// EncodeTo implements types.ProtocolObject.
func (fm *FileMetadata) EncodeTo(e *types.Encoder) {
	key, _ := hex.DecodeString(strings.TrimPrefix(fm.Key.String(), "key:"))
	e.Write(key[:])
	e.WriteString(fm.Path)
	e.WritePrefix(len(fm.Slabs))
	for _, s := range fm.Slabs {
		key, _ := hex.DecodeString(strings.TrimPrefix(s.Key.String(), "key:"))
		e.Write(key[:])
		e.WriteUint64(uint64(s.MinShards))
		e.WriteUint64(uint64(s.Offset))
		e.WriteUint64(uint64(s.Length))
		e.WritePrefix(len(s.Shards))
		for _, ss := range s.Shards {
			e.Write(ss.Host[:])
			e.Write(ss.Root[:])
		}
	}
}

// DecodeFrom implements types.ProtocolObject.
func (fm *FileMetadata) DecodeFrom(d *types.Decoder) {
	var key types.Hash256
	d.Read(key[:])
	fm.Key.UnmarshalText([]byte(strings.TrimPrefix(key.String(), "h:")))
	fm.Path = d.ReadString()
	fm.Slabs = make([]object.SlabSlice, d.ReadPrefix())
	for i := 0; i < len(fm.Slabs); i++ {
		var key types.Hash256
		d.Read(key[:])
		fm.Slabs[i].Key.UnmarshalText([]byte(strings.TrimPrefix(key.String(), "h:")))
		fm.Slabs[i].MinShards = uint8(d.ReadUint64())
		fm.Slabs[i].Offset = uint32(d.ReadUint64())
		fm.Slabs[i].Length = uint32(d.ReadUint64())
		fm.Slabs[i].Shards = make([]object.Sector, d.ReadPrefix())
		for j := 0; j < len(fm.Slabs[i].Shards); j++ {
			d.Read(fm.Slabs[i].Shards[j].Host[:])
			d.Read(fm.Slabs[i].Shards[j].Root[:])
		}
	}
}

// EncodeTo implements types.ProtocolObject.
func (smr *saveMetadataRequest) EncodeTo(e *types.Encoder) {
	smr.EncodeToWithoutSignature(e)
	smr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (smr *saveMetadataRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(smr.PubKey[:])
	smr.Metadata.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (smr *saveMetadataRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (rf *renterFiles) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (rf *renterFiles) DecodeFrom(d *types.Decoder) {
	num := d.ReadUint64()
	rf.metadata = make([]FileMetadata, 0, num)
	for num > 0 {
		var fm FileMetadata
		fm.DecodeFrom(d)
		rf.metadata = append(rf.metadata, fm)
		num--
	}
}

// EncodeTo implements types.ProtocolObject.
func (rmr *requestMetadataRequest) EncodeTo(e *types.Encoder) {
	rmr.EncodeToWithoutSignature(e)
	rmr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rmr *requestMetadataRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rmr.PubKey[:])
	e.WritePrefix(len(rmr.PresentObjects))
	for _, po := range rmr.PresentObjects {
		e.WriteString(po)
	}
}

// DecodeFrom implements types.ProtocolObject.
func (rmr *requestMetadataRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (usr *updateSlabRequest) EncodeTo(e *types.Encoder) {
	usr.EncodeToWithoutSignature(e)
	usr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (usr *updateSlabRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(usr.PubKey[:])
	key, _ := hex.DecodeString(strings.TrimPrefix(usr.Slab.Key.String(), "key:"))
	e.Write(key[:])
	e.WriteUint64(uint64(usr.Slab.MinShards))
	e.WriteUint64(uint64(usr.Slab.Offset))
	e.WriteUint64(uint64(usr.Slab.Length))
	e.WritePrefix(len(usr.Slab.Shards))
	for _, ss := range usr.Slab.Shards {
		e.Write(ss.Host[:])
		e.Write(ss.Root[:])
	}
}

// DecodeFrom implements types.ProtocolObject.
func (usr *updateSlabRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// EncodeTo implements types.ProtocolObject.
func (sr *shareRequest) EncodeTo(e *types.Encoder) {
	sr.EncodeToWithoutSignature(e)
	sr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (sr *shareRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(sr.PubKey[:])
	e.WritePrefix(len(sr.Contracts))
	for _, contract := range sr.Contracts {
		e.Write(contract.ID[:])
		e.Write(contract.HostKey[:])
		e.WriteUint64(contract.StartHeight)
		e.Write(contract.RenewedFrom[:])
		contract.Spending.Uploads.EncodeTo(e)
		contract.Spending.Downloads.EncodeTo(e)
		contract.Spending.FundAccount.EncodeTo(e)
		contract.TotalCost.EncodeTo(e)
		contract.Revision.EncodeTo(e)
	}
}

// DecodeFrom implements types.ProtocolObject.
func (sr *shareRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}
