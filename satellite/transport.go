package satellite

import (
	"context"
	"net"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

// dial makes a connection to the host.
func dial(ctx context.Context, hostIP string, hostKey types.PublicKey) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	return conn, err
}

// withTransportV2 calls the specified func on a RHP2 transport.
func (s *Satellite) withTransportV2(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv2.Transport) error) (err error) {
	conn, err := dial(ctx, hostIP, hostKey)
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
