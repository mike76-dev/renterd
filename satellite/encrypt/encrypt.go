package encrypt

import (
	"io"

	"go.sia.tech/renterd/object"
)

// RangeReader encrypts an incoming byte stream.
type RangeReader struct {
	r io.Reader
	c *Cipher
}

// NewRangeReader returns a new RangeReader.
func NewRangeReader(r io.Reader, c *Cipher) *RangeReader {
	return &RangeReader{r, c}
}

// Read implements io.Reader.
func (r *RangeReader) Read(dst []byte) (total int, err error) {
	buf := make([]byte, len(dst))
	n, err := r.r.Read(buf)
	if n > 0 {
		if n < len(buf) {
			buf = buf[:n]
		}
		r.c.XORKeyStream(buf, buf)
		copy(dst[:n], buf)
		total += n
	}

	return
}

// RangeWriter decrypts the incoming data and puts it into a stream.
type RangeWriter struct {
	w           io.Writer
	c           *Cipher
	parts       []uint64
	currentPart int
	bytesRead   uint64
}

// NewRangeWriter returns a new RangeWriter.
func NewRangeWriter(w io.Writer, c *Cipher, parts []uint64) *RangeWriter {
	return &RangeWriter{w, c, parts, 0, 0}
}

// Write implements io.Writer.
func (w *RangeWriter) Write(src []byte) (total int, err error) {
	for len(src) > 0 {
		var size int
		if len(w.parts) > 0 {
			if w.currentPart >= len(w.parts) {
				panic("data is larger than expected")
			}
			size = int(w.parts[w.currentPart] - w.bytesRead)
			if size > len(src) {
				size = len(src)
			}
		} else {
			size = len(src)
		}

		if size > 0 {
			buf := make([]byte, size)
			copy(buf, src[:size])
			w.c.XORKeyStream(buf, buf)
			n, err := w.w.Write(buf)
			if err != nil {
				return total, err
			}
			total += n
			w.bytesRead += uint64(n)
			src = src[n:]
			if len(w.parts) > 0 && w.bytesRead >= w.parts[w.currentPart] {
				// Next part: reset the state.
				w.c.Reset()
				w.bytesRead = 0
				w.currentPart++
			}
		}
	}

	return total, nil
}

// Encrypt returns a RangeReader that encrypts r.
func Encrypt(r io.Reader, key object.EncryptionKey) (*RangeReader, error) {
	ec, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 24)
	c, _ := NewUnauthenticatedCipher(ec, nonce)
	rr := NewRangeReader(r, c)

	return rr, nil
}

// Decrypt returns a RangeWriter that decrypts w.
func Decrypt(w io.Writer, key object.EncryptionKey, parts []uint64) (*RangeWriter, error) {
	ec, err := key.MarshalBinary()
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 24)
	c, _ := NewUnauthenticatedCipher(ec, nonce)
	rw := NewRangeWriter(w, c, parts)

	return rw, nil
}
