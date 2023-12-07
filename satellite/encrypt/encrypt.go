package encrypt

import (
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"

	"go.sia.tech/renterd/object"
)

// RangeReader encrypts an incoming byte stream.
type RangeReader struct {
	r             io.Reader
	c             *Cipher
	length        uint64
	lengthWritten bool
}

// NewRangeReader returns a new RangeReader.
func NewRangeReader(r io.Reader, c *Cipher, length uint64) *RangeReader {
	return &RangeReader{r, c, length, false}
}

// Read implements io.Reader.
func (r *RangeReader) Read(dst []byte) (total int, err error) {
	// Encode data length.
	if !r.lengthWritten {
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, r.length)
		r.c.XORKeyStream(l, l)
		copy(dst[:8], l)
		dst = dst[8:]
		total = 8
		r.lengthWritten = true
	}

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
	w         io.Writer
	c         *Cipher
	length    uint64
	bytesRead uint64
}

// NewRangeWriter returns a new RangeWriter.
func NewRangeWriter(w io.Writer, c *Cipher) *RangeWriter {
	return &RangeWriter{w, c, 0, 0}
}

// Write implements io.Writer.
func (w *RangeWriter) Write(src []byte) (total int, err error) {
	for len(src) > 0 {
		// Decode part length.
		if w.length == 0 && w.bytesRead == 0 {
			if len(src) < 8 {
				return total, io.EOF
			}
			l := make([]byte, 8)
			copy(l, src[:8])
			w.c.XORKeyStream(l, l)
			w.length = binary.LittleEndian.Uint64(l)
			src = src[8:]
			total += 8
		}

		// Read 'length' bytes of data.
		// If length is zero, read till the end.
		var size int
		if w.length > 0 && w.length-w.bytesRead < uint64(len(src)) {
			size = int(w.length - w.bytesRead)
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
			if w.length > 0 && w.bytesRead >= w.length {
				// Next part: reset the state.
				w.c.Reset()
				w.bytesRead = 0
				w.length = 0
			}
		}
	}

	return total, nil
}

// Encrypt returns a RangeReader that encrypts r with the given length.
func Encrypt(r io.Reader, key object.EncryptionKey, length uint64) (*RangeReader, error) {
	s := strings.TrimPrefix(key.String(), "key:")
	ec, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 24)
	c, _ := NewUnauthenticatedCipher(ec, nonce)
	rr := NewRangeReader(r, c, length)

	return rr, nil
}

// Decrypt returns a RangeWriter that decrypts w.
func Decrypt(w io.Writer, key object.EncryptionKey) (*RangeWriter, error) {
	s := strings.TrimPrefix(key.String(), "key:")
	ec, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, 24)
	c, _ := NewUnauthenticatedCipher(ec, nonce)
	rw := NewRangeWriter(w, c)

	return rw, nil
}
