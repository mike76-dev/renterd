package client

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/v2/api"
)

// Bucket returns information about a specific bucket.
func (c *Client) Bucket(ctx context.Context, bucketName string) (resp api.Bucket, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/bucket/%s", bucketName), &resp)
	return
}

// CreateBucket creates a new bucket.
func (c *Client) CreateBucket(ctx context.Context, bucketName string, opts api.CreateBucketOptions) error {
	return c.c.POST(ctx, "/buckets", api.BucketCreateRequest{
		Name:   bucketName,
		Policy: opts.Policy,
	}, nil)
}

// DeleteBucket deletes an existing bucket. Fails if the bucket isn't empty.
func (c *Client) DeleteBucket(ctx context.Context, bucketName string) error {
	return c.c.DELETE(ctx, fmt.Sprintf("/bucket/%s", bucketName))
}

// ListBuckets lists all available buckets.
func (c *Client) ListBuckets(ctx context.Context) (buckets []api.Bucket, err error) {
	err = c.c.GET(ctx, "/buckets", &buckets)
	return
}

// UpdateBucketPolicy updates the policy of an existing bucket.
func (c *Client) UpdateBucketPolicy(ctx context.Context, bucketName string, policy api.BucketPolicy) error {
	return c.c.PUT(ctx, fmt.Sprintf("/bucket/%s/policy", bucketName), api.BucketUpdatePolicyRequest{
		Policy: policy,
	})
}
