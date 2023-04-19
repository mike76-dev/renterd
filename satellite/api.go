package satellite

import (
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
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

// ContractAddRequest is the request type for the /contract request.
type ContractAddRequest struct {
	FCID types.FileContractID `json:"id"`
	PK   types.PublicKey      `json:"publickey"`
}

// ContractsAllResponse is the response type for the /all request.
type ContractsAllResponse struct {
	Contracts map[types.FileContractID]types.PublicKey `json:"contracts"`
}

// Config contains the satellite configuration parameters.
type Config struct {
	Enabled    bool            `json:"enabled"`
	Address    string          `json:"address"`
	PublicKey  types.PublicKey `json:"publicKey"`
	RenterSeed []byte          `json:"renterSeed"`
}
