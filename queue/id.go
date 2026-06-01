package queue

import (
	crand "crypto/rand"
	"time"
)

// crockford is the Crockford base32 alphabet used by ULID. It is the canonical
// ULID encoding: 32 symbols, no padding, and chosen so the textual form sorts
// identically to the underlying 128-bit value.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// idLen is the fixed width of an encoded id: 48-bit timestamp in 10 chars plus
// 80-bit randomness in 16 chars.
const idLen = 26

// newID returns a ULID-style identifier: a 48-bit millisecond timestamp followed
// by 80 bits of randomness, Crockford-base32 encoded into a fixed-width, lexically
// sortable, 26-character string. Lexical order tracks creation time (to the
// millisecond), which is what gives the queue its approximate-FIFO discovery
// order; the randomness keeps ids unique within a millisecond and free of any
// hot shared counter. The clock is injected so tests can drive ordering
// deterministically.
func newID(now time.Time) (string, error) {
	var ent [10]byte
	if _, err := crand.Read(ent[:]); err != nil {
		return "", err
	}
	ms := uint64(now.UnixMilli())

	var b [idLen]byte
	// 48-bit timestamp → 10 chars (top 2 bits of the first char are always 0,
	// since 48 bits span only 9.6 chars).
	b[0] = crockford[(ms>>45)&0x1f]
	b[1] = crockford[(ms>>40)&0x1f]
	b[2] = crockford[(ms>>35)&0x1f]
	b[3] = crockford[(ms>>30)&0x1f]
	b[4] = crockford[(ms>>25)&0x1f]
	b[5] = crockford[(ms>>20)&0x1f]
	b[6] = crockford[(ms>>15)&0x1f]
	b[7] = crockford[(ms>>10)&0x1f]
	b[8] = crockford[(ms>>5)&0x1f]
	b[9] = crockford[ms&0x1f]

	// 80-bit randomness → 16 chars, packed exactly as the ULID spec lays out the
	// entropy bytes.
	b[10] = crockford[(ent[0]&0xf8)>>3]
	b[11] = crockford[((ent[0]&0x07)<<2)|((ent[1]&0xc0)>>6)]
	b[12] = crockford[(ent[1]&0x3e)>>1]
	b[13] = crockford[((ent[1]&0x01)<<4)|((ent[2]&0xf0)>>4)]
	b[14] = crockford[((ent[2]&0x0f)<<1)|((ent[3]&0x80)>>7)]
	b[15] = crockford[(ent[3]&0x7c)>>2]
	b[16] = crockford[((ent[3]&0x03)<<3)|((ent[4]&0xe0)>>5)]
	b[17] = crockford[ent[4]&0x1f]
	b[18] = crockford[(ent[5]&0xf8)>>3]
	b[19] = crockford[((ent[5]&0x07)<<2)|((ent[6]&0xc0)>>6)]
	b[20] = crockford[(ent[6]&0x3e)>>1]
	b[21] = crockford[((ent[6]&0x01)<<4)|((ent[7]&0xf0)>>4)]
	b[22] = crockford[((ent[7]&0x0f)<<1)|((ent[8]&0x80)>>7)]
	b[23] = crockford[(ent[8]&0x7c)>>2]
	b[24] = crockford[((ent[8]&0x03)<<3)|((ent[9]&0xe0)>>5)]
	b[25] = crockford[ent[9]&0x1f]

	return string(b[:]), nil
}
