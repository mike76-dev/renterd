package bus

import (
	"encoding/hex"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
)

// SatelliteStore stores the satellite persistence.
type SatelliteStore interface {
	Config() api.SatelliteConfig
	SetConfig(c api.SatelliteConfig) error
	Contracts() map[types.FileContractID]types.PublicKey
	Satellite(types.FileContractID) (types.PublicKey, bool)
	AddContract(types.FileContractID, types.PublicKey) error
	DeleteContract(types.FileContractID) error
	DeleteAll() error
}

// SatelliteConfig returns the satellite's current configuration.
func (b *bus) SatelliteConfig() api.SatelliteConfig {
	return b.sats.Config()
}

// SetSatelliteConfig updates the satellite's configuration.
func (b *bus) SetSatelliteConfig(c api.SatelliteConfig) error {
	return b.sats.SetConfig(c)
}

// satelliteConfig simplifies the data transfer over HTTP.
type satelliteConfig struct {
	Enabled    bool   `json:"enabled"`
	Address    string `json:"address"`
	PublicKey  string `json:"publicKey"`
	RenterSeed string `json:"renterSeed"`
}

// satelliteConfigHandlerGET handles the /satellite/config requests.
func (b *bus) satelliteConfigHandlerGET(jc jape.Context) {
	c := b.SatelliteConfig()
	sc := satelliteConfig{
		Enabled:    c.Enabled,
		Address:    c.Address,
		PublicKey:  hex.EncodeToString(c.PublicKey[:]),
		RenterSeed: hex.EncodeToString(c.RenterSeed),
	}
	jc.Encode(sc)
}

// satelliteConfigHandlerPUT handles the /satellite/config requests.
func (b *bus) satelliteConfigHandlerPUT(jc jape.Context) {
	var sc satelliteConfig
	if jc.Decode(&sc) != nil {
		return
	}
	c := api.SatelliteConfig{
		Enabled: sc.Enabled,
		Address: sc.Address,
	}
	pk, _ := hex.DecodeString(sc.PublicKey)
	copy(c.PublicKey[:], pk)
	c.RenterSeed, _ = hex.DecodeString(sc.RenterSeed)
	if jc.Check("failed to set config", b.SetSatelliteConfig(c)) != nil {
		return
	}
}

// SatelliteConfig returns the satellite's current configuration.
func (c *Client) SatelliteConfig() (cfg api.SatelliteConfig, err error) {
	err = c.c.GET("/satellite/config", &cfg)
	return
}

// SetSatelliteConfig updates the satellite's configuration.
func (c *Client) SetSatelliteConfig(cfg api.SatelliteConfig) error {
	return c.c.PUT("/satellite/config", cfg)
}

// satelliteFindHandler handles the /satellite/find requests.
func (b *bus) satelliteFindHandler(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	pk, exists := b.sats.Satellite(id)
	if !exists {
		pk = types.PublicKey{}
	}
	s := api.SatelliteResponse{
		Satellite: pk,
	}
	jc.Encode(s)
}

// Satellite returns the public key of the satellite that formed the contract.
func (c *Client) Satellite(fcid types.FileContractID) (pk types.PublicKey, err error) {
	err = c.c.GET(fmt.Sprintf("/satellite/find/%s", fcid), &pk)
	return
}
