package satellite

import (
	"encoding/hex"
	"strings"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// requestRequest is used to request existing contracts.
type requestRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
}

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

	UploadPacking bool

	Signature types.Signature
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
	e.WriteBool(fr.UploadPacking)
}

// DecodeFrom implements types.ProtocolObject.
func (fr *formRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
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

	UploadPacking bool

	Signature types.Signature
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
	e.WriteBool(rr.UploadPacking)
}

// DecodeFrom implements types.ProtocolObject.
func (rr *renewRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
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

// extendedContract contains the contract and its metadata.
type extendedContract struct {
	contract            rhpv2.ContractRevision
	startHeight         uint64
	contractPrice       types.Currency
	totalCost           types.Currency
	uploadSpending      types.Currency
	downloadSpending    types.Currency
	fundAccountSpending types.Currency
	renewedFrom         types.FileContractID
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
	ec.contractPrice.DecodeFrom(d)
	ec.totalCost.DecodeFrom(d)
	ec.uploadSpending.DecodeFrom(d)
	ec.downloadSpending.DecodeFrom(d)
	ec.fundAccountSpending.DecodeFrom(d)
	ec.renewedFrom.DecodeFrom(d)
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
	for num > 0 {
		var ec extendedContract
		ec.DecodeFrom(d)
		ecs.contracts = append(ecs.contracts, ec)
		num--
	}
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

// getSettingsRequest is used to retrieve the renter's opt-in settings.
type getSettingsRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
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
	settings.ProxyUploads = d.ReadBool()
}

// updateSettingsRequest is used to update the renter's opt-in settings.
type updateSettingsRequest struct {
	PubKey             types.PublicKey
	AutoRenewContracts bool
	BackupFileMetadata bool
	AutoRepairFiles    bool
	ProxyUploads       bool
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

	UploadPacking bool

	Signature types.Signature
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
	e.WriteBool(usr.ProxyUploads)
	if usr.AutoRenewContracts || usr.BackupFileMetadata || usr.AutoRepairFiles || usr.ProxyUploads {
		e.WriteBytes(usr.SecretKey)
	}
	if usr.BackupFileMetadata || usr.AutoRepairFiles || usr.ProxyUploads {
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
		e.WriteBool(usr.UploadPacking)
	}
}

// DecodeFrom implements types.ProtocolObject.
func (usr *updateSettingsRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// revisionHash is used to read the revision hash provided by the
// satellite.
type revisionHash struct {
	RevisionHash types.Hash256
}

// EncodeTo implements types.ProtocolObject.
func (rh *revisionHash) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (rh *revisionHash) DecodeFrom(d *types.Decoder) {
	rh.RevisionHash.DecodeFrom(d)
}

// renterSignature is used to send the revision signature to the
// satellite.
type renterSignature struct {
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (rs *renterSignature) EncodeTo(e *types.Encoder) {
	rs.Signature.EncodeTo(e)
}

// DecodeFrom implements types.ProtocolObject.
func (rs *renterSignature) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// encodedFileMetadata contains the file metadata with certain
// fields encoded.
type encodedFileMetadata struct {
	Key       object.EncryptionKey `json:"key"`
	Bucket    []byte               `json:"bucket"`
	Path      []byte               `json:"path"`
	ETag      string               `json:"etag"`
	MimeType  []byte               `json:"mime"`
	Encrypted string               `json:"encrypted"`
	Slabs     []object.SlabSlice   `json:"slabs"`
	Data      []byte               `json:"data"`
}

// EncodeTo implements types.ProtocolObject.
func (fm *encodedFileMetadata) EncodeTo(e *types.Encoder) {
	key, _ := hex.DecodeString(strings.TrimPrefix(fm.Key.String(), "key:"))
	e.Write(key[:])
	e.WriteBytes(fm.Bucket)
	e.WriteBytes(fm.Path)
	e.WriteString(fm.ETag)
	e.WriteBytes(fm.MimeType)
	e.WriteString(fm.Encrypted)
	e.WritePrefix(len(fm.Slabs))
	for _, s := range fm.Slabs {
		key, _ := hex.DecodeString(strings.TrimPrefix(s.Key.String(), "key:"))
		e.Write(key[:])
		e.WriteUint64(uint64(s.MinShards))
		e.WriteUint64(uint64(s.Offset))
		e.WriteUint64(uint64(s.Length))
		e.WriteBool(s.IsPartial())
		e.WritePrefix(len(s.Shards))
		for _, ss := range s.Shards {
			e.Write(ss.LatestHost[:])
			e.Write(ss.Root[:])
		}
	}
}

// DecodeFrom implements types.ProtocolObject.
func (fm *encodedFileMetadata) DecodeFrom(d *types.Decoder) {
	var key types.Hash256
	d.Read(key[:])
	fm.Key.UnmarshalText([]byte(strings.TrimPrefix(key.String(), "h:")))
	fm.Bucket = d.ReadBytes()
	fm.Path = d.ReadBytes()
	fm.ETag = d.ReadString()
	fm.MimeType = d.ReadBytes()
	fm.Encrypted = d.ReadString()
	numSlabs := d.ReadPrefix()
	for i := 0; i < numSlabs; i++ {
		var k types.Hash256
		d.Read(k[:])
		var key object.EncryptionKey
		key.UnmarshalText([]byte(strings.TrimPrefix(k.String(), "h:")))
		minShards := uint8(d.ReadUint64())
		offset := uint32(d.ReadUint64())
		length := uint32(d.ReadUint64())
		partial := d.ReadBool()
		numShards := d.ReadPrefix()
		s := object.SlabSlice{
			Slab: object.Slab{
				Key:       key,
				MinShards: minShards,
			},
			Offset: offset,
			Length: length,
		}
		if !partial {
			s.Shards = make([]object.Sector, numShards)
			for j := 0; j < numShards; j++ {
				d.Read(s.Shards[j].LatestHost[:])
				d.Read(s.Shards[j].Root[:])
			}
		}
		fm.Slabs = append(fm.Slabs, s)
	}
}

// saveMetadataRequest is used to save file metadata on the satellite.
type saveMetadataRequest struct {
	PubKey    types.PublicKey
	Metadata  encodedFileMetadata
	Signature types.Signature
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
	e.WriteUint64(uint64(len(smr.Metadata.Data)))
}

// DecodeFrom implements types.ProtocolObject.
func (smr *saveMetadataRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// renterFiles is a collection of FileMetadata.
type renterFiles struct {
	metadata []encodedFileMetadata
	more     bool
}

// EncodeTo implements types.ProtocolObject.
func (rf *renterFiles) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (rf *renterFiles) DecodeFrom(d *types.Decoder) {
	num := d.ReadUint64()
	rf.metadata = make([]encodedFileMetadata, 0, num)
	for num > 0 {
		var fm encodedFileMetadata
		fm.DecodeFrom(d)
		dataSize := d.ReadUint64()
		if dataSize > 0 {
			fm.Data = make([]byte, dataSize)
		}
		rf.metadata = append(rf.metadata, fm)
		num--
	}
	rf.more = d.ReadBool()
}

// encodedBucketFiles contains a list of filepaths within a single bucket.
type encodedBucketFiles struct {
	Name  []byte   `json:"name"`
	Paths [][]byte `json:"paths"`
}

// requestMetadataRequest is used to request file metadata.
type requestMetadataRequest struct {
	PubKey         types.PublicKey
	PresentObjects []encodedBucketFiles
	Signature      types.Signature
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
		e.WriteBytes(po.Name)
		e.WritePrefix(len(po.Paths))
		for _, p := range po.Paths {
			e.WriteBytes(p)
		}
	}
}

// DecodeFrom implements types.ProtocolObject.
func (rmr *requestMetadataRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// updateSlabRequest is used to update a slab after a successful migration.
type updateSlabRequest struct {
	PubKey    types.PublicKey
	Slab      object.Slab
	Packed    bool
	Signature types.Signature
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
	e.WriteUint64(0) // Offset
	e.WriteUint64(0) // Length
	e.WriteBool(false)
	e.WritePrefix(len(usr.Slab.Shards))
	for _, ss := range usr.Slab.Shards {
		e.Write(ss.LatestHost[:])
		e.Write(ss.Root[:])
	}
	e.WriteBool(usr.Packed)
}

// DecodeFrom implements types.ProtocolObject.
func (usr *updateSlabRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// modifiedSlabs is a list of slabs.
type modifiedSlabs struct {
	slabs []object.Slab
}

// EncodeTo implements types.ProtocolObject.
func (ms *modifiedSlabs) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (ms *modifiedSlabs) DecodeFrom(d *types.Decoder) {
	ms.slabs = make([]object.Slab, d.ReadPrefix())
	for i := 0; i < len(ms.slabs); i++ {
		var k types.Hash256
		d.Read(k[:])
		var key object.EncryptionKey
		key.UnmarshalText([]byte(strings.TrimPrefix(k.String(), "h:")))
		minShards := uint8(d.ReadUint64())
		_ = d.ReadUint64() // Offset
		_ = d.ReadUint64() // Length
		_ = d.ReadBool()   // Partial flag
		numShards := d.ReadPrefix()
		s := object.Slab{
			Key:       key,
			MinShards: minShards,
		}
		s.Shards = make([]object.Sector, numShards)
		for j := 0; j < numShards; j++ {
			d.Read(s.Shards[j].LatestHost[:])
			d.Read(s.Shards[j].Root[:])
		}
		ms.slabs[i] = s
	}
}

// requestSlabsRequest is used to request modified slabs.
type requestSlabsRequest struct {
	PubKey    types.PublicKey
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (rsr *requestSlabsRequest) EncodeTo(e *types.Encoder) {
	rsr.EncodeToWithoutSignature(e)
	rsr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rsr *requestSlabsRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rsr.PubKey[:])
}

// DecodeFrom implements types.ProtocolObject.
func (rsr *requestSlabsRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// shareRequest is used to send a set of contracts to the satellite.
type shareRequest struct {
	PubKey    types.PublicKey
	Contracts []api.Contract
	Signature types.Signature
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
		contract.ContractPrice.EncodeTo(e)
		contract.TotalCost.EncodeTo(e)
		contract.Revision.EncodeTo(e)
	}
}

// DecodeFrom implements types.ProtocolObject.
func (sr *shareRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// uploadRequest is used to upload a file to the satellite via RHP3.
type uploadRequest struct {
	PubKey    types.PublicKey
	Bucket    []byte
	Path      []byte
	MimeType  []byte
	Encrypted bool
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (ur *uploadRequest) EncodeTo(e *types.Encoder) {
	ur.EncodeToWithoutSignature(e)
	ur.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (ur *uploadRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(ur.PubKey[:])
	e.WriteBytes(ur.Bucket)
	e.WriteBytes(ur.Path)
	e.WriteBytes(ur.MimeType)
	e.WriteBool(ur.Encrypted)
}

// DecodeFrom implements types.ProtocolObject.
func (ur *uploadRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// uploadResponse is the response type for uploadRequest.
type uploadResponse struct {
	DataSize uint64
}

// EncodeTo implements types.ProtocolObject.
func (ur *uploadResponse) EncodeTo(e *types.Encoder) {
	e.WriteUint64(ur.DataSize)
}

// DecodeFrom implements types.ProtocolObject.
func (ur *uploadResponse) DecodeFrom(d *types.Decoder) {
	ur.DataSize = d.ReadUint64()
}

// uploadData contains a chunk of data and an indicator if there
// is more data available.
type uploadData struct {
	Data []byte
	More bool
}

// EncodeTo implements types.ProtocolObject.
func (ud *uploadData) EncodeTo(e *types.Encoder) {
	e.WriteBytes(ud.Data)
	e.WriteBool(ud.More)
}

// DecodeFrom implements types.ProtocolObject.
func (ud *uploadData) DecodeFrom(d *types.Decoder) {
	ud.Data = d.ReadBytes()
	ud.More = d.ReadBool()
}

// registerMultipartRequest is used to register a new S3 multipart upload.
type registerMultipartRequest struct {
	PubKey    types.PublicKey
	Key       object.EncryptionKey
	Bucket    []byte
	Path      []byte
	MimeType  []byte
	Encrypted bool
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (rmr *registerMultipartRequest) EncodeTo(e *types.Encoder) {
	rmr.EncodeToWithoutSignature(e)
	rmr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (rmr *registerMultipartRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(rmr.PubKey[:])
	key, _ := hex.DecodeString(strings.TrimPrefix(rmr.Key.String(), "key:"))
	e.Write(key[:])
	e.WriteBytes(rmr.Bucket)
	e.WriteBytes(rmr.Path)
	e.WriteBytes(rmr.MimeType)
	e.WriteBool(rmr.Encrypted)
}

// DecodeFrom implements types.ProtocolObject.
func (rmr *registerMultipartRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// registerMultipartResponse is the response type for registerMultipartRequest.
type registerMultipartResponse struct {
	UploadID types.Hash256
}

// EncodeTo implements types.ProtocolObject.
func (rmr *registerMultipartResponse) EncodeTo(e *types.Encoder) {
	// Nothing to do here.
}

// DecodeFrom implements types.ProtocolObject.
func (rmr *registerMultipartResponse) DecodeFrom(d *types.Decoder) {
	d.Read(rmr.UploadID[:])
}

// deleteMultipartRequest is used to abort an incomplete S3 multipart upload.
type deleteMultipartRequest struct {
	PubKey    types.PublicKey
	UploadID  types.Hash256
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (dmr *deleteMultipartRequest) EncodeTo(e *types.Encoder) {
	dmr.EncodeToWithoutSignature(e)
	dmr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (dmr *deleteMultipartRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(dmr.PubKey[:])
	e.Write(dmr.UploadID[:])
}

// DecodeFrom implements types.ProtocolObject.
func (dmr *deleteMultipartRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// uploadPartRequest is used to upload a part of a multipart upload
// to the satellite via RHP3.
type uploadPartRequest struct {
	PubKey     types.PublicKey
	UploadID   types.Hash256
	PartNumber int
	Signature  types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (upr *uploadPartRequest) EncodeTo(e *types.Encoder) {
	upr.EncodeToWithoutSignature(e)
	upr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (upr *uploadPartRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(upr.PubKey[:])
	e.Write(upr.UploadID[:])
	e.WriteUint64(uint64(upr.PartNumber))
}

// DecodeFrom implements types.ProtocolObject.
func (upr *uploadPartRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}

// completeMultipartRequest is used to complete an S3 multipart upload.
type completeMultipartRequest struct {
	PubKey    types.PublicKey
	UploadID  types.Hash256
	Signature types.Signature
}

// EncodeTo implements types.ProtocolObject.
func (cmr *completeMultipartRequest) EncodeTo(e *types.Encoder) {
	cmr.EncodeToWithoutSignature(e)
	cmr.Signature.EncodeTo(e)
}

// EncodeToWithoutSignature does the same as EncodeTo but
// leaves the signature out.
func (cmr *completeMultipartRequest) EncodeToWithoutSignature(e *types.Encoder) {
	e.Write(cmr.PubKey[:])
	e.Write(cmr.UploadID[:])
}

// DecodeFrom implements types.ProtocolObject.
func (cmr *completeMultipartRequest) DecodeFrom(d *types.Decoder) {
	// Nothing to do here.
}
