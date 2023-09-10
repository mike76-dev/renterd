package satellite

import (
	"context"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// A Client provides methods for interacting with an API server.
type Client struct {
	c jape.Client
}

// RequestContracts requests the existing active contracts from the
// satellite and adds them to the contract set.
func (c *Client) RequestContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).GET("/request", &resp)
	return resp, err
}

// ShareContracts sends the contract set to the satellite.
func (c *Client) ShareContracts(ctx context.Context) error {
	err := c.c.WithContext(ctx).POST("/contracts", nil, nil)
	return err
}

// FormContracts requests the satellite to form the specified number
// of contracts with the hosts and adds them to the contract set.
func (c *Client) FormContracts(ctx context.Context, hosts uint64, period uint64, renewWindow uint64, storage uint64, upload uint64, download uint64) ([]api.ContractMetadata, error) {
	req := FormRequest{
		Hosts:       hosts,
		Period:      period,
		RenewWindow: renewWindow,
		Download:    download,
		Upload:      upload,
		Storage:     storage,
	}
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/form", req, &resp)
	return resp, err
}

// RenewContracts requests the satellite to renew the given set
// of contracts and add them to the contract set.
func (c *Client) RenewContracts(ctx context.Context, contracts []types.FileContractID, period uint64, renewWindow uint64, storage uint64, upload uint64, download uint64) ([]api.ContractMetadata, error) {
	req := RenewRequest{
		Contracts:   contracts,
		Period:      period,
		RenewWindow: renewWindow,
		Download:    download,
		Upload:      upload,
		Storage:     storage,
	}
	var resp []api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/renew", req, &resp)
	return resp, err
}

// UpdateRevision submits an updated contract revision to the satellite.
func (c *Client) UpdateRevision(ctx context.Context, rev rhpv2.ContractRevision, spending api.ContractSpending) error {
	req := UpdateRevisionRequest{
		Revision: rev,
		Spending: spending,
	}
	err := c.c.WithContext(ctx).POST("/update", req, nil)
	return err
}

// Config returns the satellite's current configuration.
func (c *Client) Config() (cfg Config, err error) {
	err = c.c.GET("/config", &cfg)
	return
}

// SetConfig updates the satellite's configuration.
func (c *Client) SetConfig(cfg Config) error {
	return c.c.PUT("/config", cfg)
}

// AddContract adds a contract to the store.
func (c *Client) AddContract(fcid types.FileContractID, pk types.PublicKey) error {
	car := ContractAddRequest{
		FCID: fcid,
		PK:   pk,
	}
	return c.c.PUT("/contract", &car)
}

// DeleteContract removes a contract from the store.
func (c *Client) DeleteContract(fcid types.FileContractID) error {
	return c.c.DELETE(fmt.Sprintf("/contract/%s", fcid))
}

// FindSatellite returns the public key of the satellite that formed the contract.
func (c *Client) FindSatellite(fcid types.FileContractID) (pk types.PublicKey, err error) {
	err = c.c.GET(fmt.Sprintf("/contract/%s", fcid), &pk)
	return
}

// GetContracts returns all satellite contracts.
func (c *Client) GetContracts() (contracts map[types.FileContractID]types.PublicKey, err error) {
	err = c.c.GET("/contracts", &contracts)
	return
}

// DeleteContracts deletes all satellite contracts.
func (c *Client) DeleteContracts() error {
	return c.c.DELETE("/contracts")
}

// AddSatellite adds a satellite to the store.
func (c *Client) AddSatellite(si SatelliteInfo) error {
	return c.c.PUT("/satellite", &si)
}

// GetSatellite retrieves the satellite information.
func (c *Client) GetSatellite(pk types.PublicKey) (si SatelliteInfo, err error) {
	err = c.c.GET(fmt.Sprintf("/satellite/%s", pk), &si)
	return
}

// GetContracts returns all satellite contracts.
func (c *Client) GetSatellites() (satellites map[types.PublicKey]SatelliteInfo, err error) {
	err = c.c.GET("/satellites", &satellites)
	return
}

// FormContract requests the satellite to form a contract with the
// specified host and adds it to the contract set.
func (c *Client) FormContract(ctx context.Context, hpk types.PublicKey, endHeight uint64, storage uint64, upload uint64, download uint64) (api.ContractMetadata, error) {
	req := FormContractRequest{
		HostKey:   hpk,
		EndHeight: endHeight,
		Download:  download,
		Upload:    upload,
		Storage:   storage,
	}
	var resp api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/rspv2/form", req, &resp)
	return resp, err
}

// RenewContract requests the satellite to renew the specified contract
// and adds the new contract to the contract set.
func (c *Client) RenewContract(ctx context.Context, fcid types.FileContractID, endHeight uint64, storage uint64, upload uint64, download uint64) (api.ContractMetadata, error) {
	req := RenewContractRequest{
		Contract:  fcid,
		EndHeight: endHeight,
		Download:  download,
		Upload:    upload,
		Storage:   storage,
	}
	var resp api.ContractMetadata
	err := c.c.WithContext(ctx).POST("/rspv2/renew", req, &resp)
	return resp, err
}

// GetSettings retrieves the renter's opt-in settings.
func (c *Client) GetSettings(ctx context.Context) (settings RenterSettings, err error) {
	err = c.c.WithContext(ctx).GET("/settings", &settings)
	return
}

// UpdateSettings updates the renter's opt-in settings.
func (c *Client) UpdateSettings(ctx context.Context, settings RenterSettings) error {
	return c.c.WithContext(ctx).POST("/settings", &settings, nil)
}

// SaveMetadata sends the file metadata to the satellite.
func (c *Client) SaveMetadata(ctx context.Context, fm FileMetadata) error {
	req := SaveMetadataRequest{
		Metadata: fm,
	}
	err := c.c.WithContext(ctx).POST("/metadata", req, nil)
	return err
}

// RequestMetadata requests the file metadata from the satellite.
func (c *Client) RequestMetadata(ctx context.Context, set string) (objects []object.Object, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/metadata/%s", set), &objects)
	return
}

// UpdateSlab sends the updated slab to the satellite.
func (c *Client) UpdateSlab(ctx context.Context, s object.Slab) error {
	req := UpdateSlabRequest{
		Slab: s,
	}
	err := c.c.WithContext(ctx).POST("/slab", req, nil)
	return err
}

// NewClient returns a client that communicates with a renterd satellite server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}
