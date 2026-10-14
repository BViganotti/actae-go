package actae

import (
	"crypto/sha1"
	"fmt"
)

// actaeNamespace is the fixed UUIDv5 namespace for all Actae
// deterministic operation keys.
var actaeNamespace = [16]byte{
	0x3f, 0x74, 0xe5, 0xf1, 0x9b, 0x2c, 0x4a, 0x7e,
	0x8f, 0x1d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
}

// DeterministicOperationKey returns a stable UUIDv5 operation id for the
// (scope, action, identity...) triple. Retrying the same logical operation
// — from a library retry loop or after a process restart — produces the
// exact same id, which Actae's server-side idempotency (record
// operation_id / transition replay) turns into "return the original
// result instead of duplicating".
//
// identity parts are NUL-separated so that overlapping part boundaries
// cannot collide (e.g. ("a", "b", "c") never equals ("a", "b:c", "")),
// and the string is truncated to avoids exceeding server limits on
// operation_id.
func DeterministicOperationKey(scope, action string, identity ...string) string {
	name := scope + "\x00" + action
	for _, part := range identity {
		name += "\x00" + part
	}
	if len(name) > 256 {
		// UUIDv5 hashes the whole name anyway; truncation only caps the
		// input length, never the entropy.
		name = name[:256]
	}

	h := sha1.New()
	h.Write(actaeNamespace[:])
	h.Write([]byte(name))
	digest := h.Sum(nil)

	var b [16]byte
	copy(b[:], digest[:16])
	b[6] = (b[6] & 0x0f) | 0x50 // version 5
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
