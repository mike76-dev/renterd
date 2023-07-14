//go:build !testnet

package build

import (
	"time"

	"go.sia.tech/core/chain"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const (
	ConsensusNetworkName  = "Mainnet"
	DefaultAPIAddress     = "localhost:9980"
	DefaultGatewayAddress = ":9981"
)

var (
	ConsensusNetwork, _ = chain.Mainnet()

	// DefaultGougingSettings define the default gouging settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultGougingSettings = api.GougingSettings{
		MinMaxCollateral:              types.Siacoins(10),                                  // at least up to 10 SC per contract
		MaxRPCPrice:                   types.Siacoins(1).Div64(1000),                       // 1mS per RPC
		MaxContractPrice:              types.Siacoins(15),                                  // 15 SC per contract
		MaxDownloadPrice:              types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxUploadPrice:                types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxStoragePrice:               types.Siacoins(3000).Div64(1 << 40).Div64(144 * 30), // 3000 SC per TiB per month
		HostBlockHeightLeeway:         6,                                                   // 6 blocks
		MinPriceTableValidity:         5 * time.Minute,                                     // 5 minutes
		MinAccountExpiry:              24 * time.Hour,                                      // 1 day
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                                   // 1 SC
	}

	// DefaultPartialUploadSettings define the default partial upload settings
	// the bus is configured with on startup.
	DefaultPartialUploadSettings = api.PartialUploadSettings{
		Enabled: false,
	}

	// DefaultRedundancySettings define the default redundancy settings the bus
	// is configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultRedundancySettings = api.RedundancySettings{
		MinShards:   10,
		TotalShards: 30,
	}
)
