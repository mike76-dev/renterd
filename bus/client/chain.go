package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

// AcceptBlock submits a block to the consensus manager.
func (c *Client) AcceptBlock(ctx context.Context, b types.Block) (err error) {
	err = c.c.POST(ctx, "/consensus/acceptblock", b, nil)
	return
}

// BroadcastTransaction broadcasts the transaction set to the network.
func (c *Client) BroadcastTransaction(ctx context.Context, txns []types.Transaction) error {
	return c.c.POST(ctx, "/txpool/broadcast", txns, nil)
}

// ConsensusNetwork returns information about the consensus network.
func (c *Client) ConsensusNetwork(ctx context.Context) (resp consensus.Network, err error) {
	err = c.c.GET(ctx, "/consensus/network", &resp)
	return
}

// ConsensusState returns the current block height and whether the node is
// synced.
func (c *Client) ConsensusState(ctx context.Context) (resp api.ConsensusState, err error) {
	err = c.c.GET(ctx, "/consensus/state", &resp)
	return
}

// FileContractTax asks the bus for the siafund fee that has to be paid for a
// contract with a given payout.
func (c *Client) FileContractTax(ctx context.Context, payout types.Currency) (tax types.Currency, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/consensus/siafundfee/%s", api.ParamCurrency(payout)), &tax)
	return
}

// RecommendedFee returns the recommended fee for a txn.
func (c *Client) RecommendedFee(ctx context.Context) (fee types.Currency, err error) {
	err = c.c.GET(ctx, "/txpool/recommendedfee", &fee)
	return
}

// SyncerAddress returns the address the syncer is listening on.
func (c *Client) SyncerAddress(ctx context.Context) (addr string, err error) {
	err = c.c.GET(ctx, "/syncer/address", &addr)
	return
}

// SyncerPeers returns the current peers of the syncer.
func (c *Client) SyncerPeers(ctx context.Context) (resp []string, err error) {
	err = c.c.GET(ctx, "/syncer/peers", &resp)
	return
}

// SyncerConnect adds the address as a peer of the syncer.
func (c *Client) SyncerConnect(ctx context.Context, addr string) (err error) {
	err = c.c.POST(ctx, "/syncer/connect", addr, nil)
	return
}

// TransactionPool returns the transactions currently in the pool.
func (c *Client) TransactionPool(ctx context.Context) (txns []types.Transaction, err error) {
	err = c.c.GET(ctx, "/txpool/transactions", &txns)
	return
}
