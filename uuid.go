package actae

import (
	"crypto/rand"
	"fmt"
)

// newUUID returns a random UUID v4 string (e.g.
// "3f2f4a72-8e6a-4d1c-9b5e-1a2b3c4d5e6f"), mirroring Python's
// str(uuid.uuid4()).
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("actae: failed to read random bytes: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
