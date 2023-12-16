package satellite

import (
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// FormRequest is the request type for the FormContracts RPC.
type FormRequest struct {
	Hosts uint64 `json:"hosts"`
	// Contract configuration (all units are blocks or bytes).
	Period      uint64 `json:"period"`
	RenewWindow uint64 `json:"renewWindow"`
	Download    uint64 `json:"download"`
	Upload      uint64 `json:"upload"`
	Storage     uint64 `json:"storage"`
}

// RenewRequest is the request type for the RenewContracts RPC.
type RenewRequest struct {
	Contracts []types.FileContractID `json:"contracts"`
	// Contract configuration (all units are blocks or bytes).
	Period      uint64 `json:"period"`
	RenewWindow uint64 `json:"renewWindow"`
	Download    uint64 `json:"download"`
	Upload      uint64 `json:"upload"`
	Storage     uint64 `json:"storage"`
}

// UpdateRevisionRequest is the request type for the UpdateRevision RPC.
type UpdateRevisionRequest struct {
	Revision rhpv2.ContractRevision `json:"revision"`
	Spending api.ContractSpending   `json:"spending"`
}

// Config contains the satellite configuration parameters.
type Config struct {
	Enabled       bool                 `json:"enabled"`
	Encrypt       bool                 `json:"encrypt"`
	EncryptionKey object.EncryptionKey `json:"encryptionKey"`
	SatelliteInfo
}

// SatelliteInfo contains the information about the satellite.
type SatelliteInfo struct {
	Address    string          `json:"address"`
	MuxPort    string          `json:"muxPort"`
	PublicKey  types.PublicKey `json:"publicKey"`
	RenterSeed []byte          `json:"renterSeed"`
}

// SatellitesAllResponse is the response type for the /satellites request.
type SatellitesAllResponse struct {
	Satellites map[types.PublicKey]SatelliteInfo `json:"satellites"`
}

// ObjectPutRequest is the request type for the PUT /object requests.
type ObjectPutRequest struct {
	Bucket string   `json:"bucket"`
	Parts  []uint64 `json:"parts"`
}

// ObjectResponse is the response type for the GET /object request.
type ObjectResponse struct {
	Found bool     `json:"found"`
	Parts []uint64 `json:"parts"`
}

// FormContractRequest is the request type for the FormContract RPC.
type FormContractRequest struct {
	HostKey types.PublicKey `json:"hostPublicKey"`
	// Contract configuration (all units are blocks or bytes).
	EndHeight uint64 `json:"endHeight"`
	Download  uint64 `json:"download"`
	Upload    uint64 `json:"upload"`
	Storage   uint64 `json:"storage"`
}

// RenewContractRequest is the request type for the RenewContract RPC.
type RenewContractRequest struct {
	Contract types.FileContractID `json:"contract"`
	// Contract configuration (all units are blocks or bytes).
	EndHeight uint64 `json:"endHeight"`
	Download  uint64 `json:"download"`
	Upload    uint64 `json:"upload"`
	Storage   uint64 `json:"storage"`
}

// RenterSettings contains the renter's opt-in settings.
type RenterSettings struct {
	AutoRenewContracts bool `json:"autoRenew"`
	BackupFileMetadata bool `json:"backupMetadata"`
	AutoRepairFiles    bool `json:"autoRepair"`
	ProxyUploads       bool `json:"proxyUploads"`
}

// FileMetadata contains the uploaded file metadata.
type FileMetadata struct {
	Key      object.EncryptionKey `json:"key"`
	Bucket   string               `json:"bucket"`
	Path     string               `json:"path"`
	ETag     string               `json:"etag"`
	MimeType string               `json:"mime"`
	Parts    []uint64             `json:"parts"`
	Slabs    []object.SlabSlice   `json:"slabs"`
	Data     []byte               `json:"data"`
}

// SaveMetadataRequest is the request type for the SaveMetadata RPC.
type SaveMetadataRequest struct {
	Metadata FileMetadata `json:"metadata"`
	New      bool         `json:"new"`
}

// UpdateSlabRequest is the request type for the UpdateSlab RPC.
type UpdateSlabRequest struct {
	Slab   object.Slab `json:"slab"`
	Packed bool        `json:"packed"`
}

// BucketFiles contains a list of filepaths within a single bucket.
type BucketFiles struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths"`
}

// CreateMultipartRequest is the request type for the CreateMultipart RPC.
type CreateMultipartRequest struct {
	Key      object.EncryptionKey `json:"key"`
	Bucket   string               `json:"bucket"`
	Path     string               `json:"path"`
	MimeType string               `json:"mime"`
}

// CreateMultipartResponse is the response type for the CreateMultipart RPC.
type CreateMultipartResponse struct {
	UploadID string `json:"uploadID"`
}
