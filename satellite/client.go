package satellite

import (
	"context"
	"fmt"
	"net/url"
	"strings"

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

// AddSatellite adds a satellite to the store.
func (c *Client) AddSatellite(si SatelliteInfo) error {
	return c.c.PUT("/satellite", &si)
}

// GetSatellite retrieves the satellite information.
func (c *Client) GetSatellite(pk types.PublicKey) (si SatelliteInfo, err error) {
	err = c.c.GET(fmt.Sprintf("/satellite/%s", pk), &si)
	return
}

// GetSatellites returns all known satellites.
func (c *Client) GetSatellites() (satellites map[types.PublicKey]SatelliteInfo, err error) {
	err = c.c.GET("/satellites", &satellites)
	return
}

// GetObject retrieves the information about an encrypted object.
func (c *Client) GetObject(bucket, path string) (or ObjectResponse, err error) {
	values := url.Values{}
	values.Set("bucket", bucket)
	escapedPath := url.PathEscape(strings.TrimPrefix(path, "/"))
	err = c.c.GET(fmt.Sprintf("/object/%s?"+values.Encode(), escapedPath), &or)
	return
}

// AddObject adds the information about an encrypted object.
func (c *Client) AddObject(bucket, path string, parts []uint64) error {
	req := ObjectPutRequest{
		Bucket: bucket,
		Parts:  parts,
	}
	escapedPath := url.PathEscape(strings.TrimPrefix(path, "/"))
	return c.c.PUT(fmt.Sprintf("/object/%s", escapedPath), &req)
}

// DeleteObject deletes the information about an encrypted object.
func (c *Client) DeleteObject(bucket, path string) error {
	values := url.Values{}
	values.Set("bucket", bucket)
	path = url.PathEscape(strings.TrimPrefix(path, "/"))
	return c.c.DELETE(fmt.Sprintf("/object/%s?"+values.Encode(), path))
}

// DeleteObjects deletes the information about a series of encrypted objects.
func (c *Client) DeleteObjects(bucket, path string) error {
	values := url.Values{}
	values.Set("bucket", bucket)
	path = url.PathEscape(strings.TrimPrefix(path, "/"))
	return c.c.DELETE(fmt.Sprintf("/objects/%s?"+values.Encode(), path))
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
func (c *Client) SaveMetadata(ctx context.Context, fm FileMetadata, isNew bool) error {
	req := SaveMetadataRequest{
		Metadata: fm,
		New:      isNew,
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
func (c *Client) UpdateSlab(ctx context.Context, s object.Slab, packed bool) error {
	req := UpdateSlabRequest{
		Slab:   s,
		Packed: packed,
	}
	err := c.c.WithContext(ctx).POST("/slab", req, nil)
	return err
}

// RequestSlabs requests any modified slabs from the satellite.
func (c *Client) RequestSlabs(ctx context.Context, set string) (slabs []object.Slab, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/slabs/%s", set), &slabs)
	return
}

// CreateMultipart registers a new multipart upload with the satellite
// and returns the upload ID.
func (c *Client) CreateMultipart(ctx context.Context, key object.EncryptionKey, bucket, path, mimeType string) (string, error) {
	req := CreateMultipartRequest{
		Key:      key,
		Bucket:   bucket,
		Path:     path,
		MimeType: mimeType,
	}
	var resp CreateMultipartResponse
	err := c.c.WithContext(ctx).POST("/multipart/create", req, &resp)
	return resp.UploadID, err
}

// AbortMultipart deletes an incomplete multipart upload on the satellite.
func (c *Client) AbortMultipart(ctx context.Context, id string) error {
	req := CreateMultipartResponse{
		UploadID: id,
	}
	err := c.c.WithContext(ctx).POST("/multipart/abort", req, nil)
	return err
}

// CompleteMultipart completes an incomplete multipart upload on the satellite.
func (c *Client) CompleteMultipart(ctx context.Context, id string) error {
	req := CreateMultipartResponse{
		UploadID: id,
	}
	err := c.c.WithContext(ctx).POST("/multipart/complete", req, nil)
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
