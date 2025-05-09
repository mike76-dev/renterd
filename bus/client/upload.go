package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

// AddUploadingSectors adds the given sectors to the upload with given id.
func (c *Client) AddUploadingSectors(ctx context.Context, uID api.UploadID, roots []types.Hash256) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/upload/%s/sector", uID), &roots, nil)
	return
}

// FinishUpload marks the given upload as finished.
func (c *Client) FinishUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.DELETE(ctx, fmt.Sprintf("/upload/%s", uID))
	return
}

// TrackUpload tracks the upload with given id in the bus.
func (c *Client) TrackUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/upload/%s", uID), nil, nil)
	return
}
