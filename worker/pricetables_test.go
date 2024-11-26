package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test/mocks"
)

func TestPriceTables(t *testing.T) {
	// create host & contract stores
	hs := mocks.NewHostStore()
	cs := mocks.NewContractStore()

	// create host manager & price table
	hm := newTestHostManager(t)
	pts := newPriceTables()

	// create host & contract mock
	hostMock := hs.AddHost()
	c := cs.AddContract(hostMock.PublicKey())

	cm := mocks.NewChain(api.ConsensusState{
		BlockHeight: 1,
	})
	cState, _ := cm.ConsensusState(context.Background())

	blockHeightLeeway := 10
	gCtx := WithGougingChecker(context.Background(), cm, api.GougingParams{
		ConsensusState: cState,
		GougingSettings: api.GougingSettings{
			HostBlockHeightLeeway: blockHeightLeeway,
		},
	})

	// expire its price table
	expiredPT := newTestHostPriceTable()
	expiredPT.Expiry = time.Now()
	hostMock.UpdatePriceTable(expiredPT)

	// manage the host, make sure fetching the price table blocks
	fetchPTBlockChan := make(chan struct{})
	validPT := newTestHostPriceTable()
	h := newTestHostCustom(hostMock, c, func() api.HostPriceTable {
		<-fetchPTBlockChan
		return validPT
	}, func() rhpv4.HostPrices {
		t.Fatal("shouldn't be called")
		return rhpv4.HostPrices{}
	})
	hm.addHost(h)

	// trigger a fetch to make it block
	go pts.fetch(gCtx, h, nil)
	time.Sleep(50 * time.Millisecond)

	// fetch it again but with a canceled context to avoid blocking
	// indefinitely, the error will indicate we were blocking on a price table
	// update
	ctx, cancel := context.WithCancel(gCtx)
	cancel()
	_, _, err := pts.fetch(ctx, h, nil)
	if !errors.Is(err, errPriceTableUpdateTimedOut) {
		t.Fatal("expected errPriceTableUpdateTimedOut, got", err)
	}

	// unblock and assert we paid for the price table
	close(fetchPTBlockChan)
	update, _, err := pts.fetch(gCtx, h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// refresh the price table on the host, update again, assert we receive the
	// same price table as it hasn't expired yet
	h.UpdatePriceTable(newTestHostPriceTable())
	update, _, err = pts.fetch(gCtx, h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != validPT.UID {
		t.Fatal("price table mismatch")
	}

	// increase the current block height to be exactly
	// 'priceTableBlockHeightLeeway' blocks before the leeway of the gouging
	// settings
	h.UpdatePriceTable(newTestHostPriceTable())
	validPT = h.HostPriceTable()
	cm.UpdateHeight(validPT.HostBlockHeight + uint64(blockHeightLeeway) - priceTableBlockHeightLeeway)

	// fetch it again and assert we updated the price table
	update, _, err = pts.fetch(gCtx, h, nil)
	if err != nil {
		t.Fatal(err)
	} else if update.UID != h.HostPriceTable().UID {
		t.Fatal("price table mismatch")
	}
}
