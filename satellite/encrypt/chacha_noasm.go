// This is a slight modification of the original
// golang.org/x/crypto/chacha20 code.

// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//go:build (!arm64 && !s390x && !ppc64le) || !gc || purego

package encrypt

const bufSize = blockSize

func (s *Cipher) xorKeyStreamBlocks(dst, src []byte) {
	s.xorKeyStreamBlocksGeneric(dst, src)
}
