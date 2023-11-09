package satellite

import (
	"context"
	"net"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

// dial makes a connection to the host.
func dial(ctx context.Context, hostIP string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	return conn, err
}

// withTransportV2 calls the specified func on a RHP2 transport.
func withTransportV2(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv2.Transport) error) (err error) {
	conn, err := dial(ctx, hostIP)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	t, err := rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()
	return fn(t)
}

// withTransportV3 calls the specified func on a RHP3 transport.
func withTransportV3(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv3.Transport) error) (err error) {
	conn, err := dial(ctx, hostIP)
	if err != nil {
		return err
	}
	var t *rhpv3.Transport
	done := make(chan struct{})
	go func() {
		t, err = rhpv3.NewRenterTransport(conn, hostKey)
		close(done)
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		<-done
		return ctx.Err()
	case <-done:
		return fn(t)
	}
}
