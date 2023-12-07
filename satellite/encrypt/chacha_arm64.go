// This is a slight modification of the original
// golang.org/x/crypto/chacha20 code.

// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build gc && !purego

package encrypt

const bufSize = 256

//go:noescape
func xorKeyStreamVX(dst, src []byte, key *[8]uint32, nonce *[3]uint32, counter *uint32)

func (c *Cipher) xorKeyStreamBlocks(dst, src []byte) {
	xorKeyStreamVX(dst, src, &c.key, &c.nonce, &c.counter)
}
