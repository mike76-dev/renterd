package api

import (
	"go.sia.tech/core/types"
)

// SatelliteFormRequest is the request type for the /satellite/form endpoint.
type SatelliteFormRequest struct {
	Hosts uint64 `json:"hosts"`
	// Contract configuration (all units are blocks or bytes).
	Period      uint64 `json:"period"`
	RenewWindow uint64 `json:"renewWindow"`
	Download    uint64 `json:"download"`
	Upload      uint64 `json:"upload"`
	Storage     uint64 `json:"storage"`
}

// SatelliteRenewRequest is the request type for the /satellite/renew endpoint.
type SatelliteRenewRequest struct {
	Contracts []types.FileContractID `json:"contracts"`
	// Contract configuration (all units are blocks or bytes).
	Period      uint64 `json:"period"`
	RenewWindow uint64 `json:"renewWindow"`
	Download    uint64 `json:"download"`
	Upload      uint64 `json:"upload"`
	Storage     uint64 `json:"storage"`
}

// SatelliteConfig contains the satellite configuration parameters.
type SatelliteConfig struct {
	Enabled    bool            `json:"enabled"`
	Address    string          `json:"address"`
	PublicKey  types.PublicKey `json:"publicKey"`
	RenterSeed []byte          `json:"renterSeed"`
}
