package engine

import (
	"bytes"
	"encoding/binary"
)

// Define our operation types
const (
	TypeDelete byte = 0
	TypePut    byte = 1
)

const internalKeySuffixLen = 9 // 8 bytes for SeqNum + 1 byte for Type

// EncodeInternalKey packs a user key, a sequence number, and an operation type into a single byte slice.
// Format: [UserKey (Variable)] + [SeqNum (8 Bytes)] + [Type (1 Byte)]
func EncodeInternalKey(userKey []byte, seqNum uint64, keyType byte) []byte {
	size := len(userKey) + internalKeySuffixLen
	buf := make([]byte, size)

	// 1. Copy the UserKey into the start of the buffer
	copy(buf, userKey)

	// 2. Encode the 64-bit SeqNum in BigEndian format right after the UserKey
	binary.BigEndian.PutUint64(buf[len(userKey):], seqNum)

	// 3. Append the 1-byte operation type at the very end
	buf[size-1] = keyType

	return buf
}

// ParseInternalKey extracts the components of an InternalKey WITHOUT allocating new memory.
// It returns a slice pointing to the original UserKey bytes.
func ParseInternalKey(internalKey []byte) (userKey []byte, seqNum uint64, keyType byte) {
	if len(internalKey) < internalKeySuffixLen {
		return nil, 0, 0 // Invalid key
	}

	suffixStart := len(internalKey) - internalKeySuffixLen

	userKey = internalKey[:suffixStart]
	seqNum = binary.BigEndian.Uint64(internalKey[suffixStart : suffixStart+8])
	keyType = internalKey[len(internalKey)-1]

	return userKey, seqNum, keyType
}

// CompareInternalKeys is the heart of our MVCC sorting.
// Rule 1: Sort by UserKey Ascending (A-Z)
// Rule 2: If UserKeys match, sort by SeqNum DESCENDING (Newest first)
func CompareInternalKeys(a, b []byte) int {
	lenA := len(a)
	lenB := len(b)

	// Safety check for malformed data
	if lenA < internalKeySuffixLen || lenB < internalKeySuffixLen {
		return 0
	}

	userKeyLenA := lenA - internalKeySuffixLen
	userKeyLenB := lenB - internalKeySuffixLen

	// FAST PATH: Compare UserKeys directly.
	// We do NOT parse the SeqNum unless the UserKeys are identical.
	cmp := bytes.Compare(a[:userKeyLenA], b[:userKeyLenB])
	if cmp != 0 {
		return cmp
	}

	// UserKeys match. Now we care about the SeqNum (descending).
	// We extract the 8 bytes and read them as uint64.
	seqNumA := binary.BigEndian.Uint64(a[userKeyLenA : userKeyLenA+8])
	seqNumB := binary.BigEndian.Uint64(b[userKeyLenB : userKeyLenB+8])

	if seqNumA > seqNumB {
		return -1 // a is newer, comes BEFORE b
	} else if seqNumA < seqNumB {
		return 1 // a is older, comes AFTER b
	}

	// SeqNums are identical. Fallback to Type.
	typeA := a[lenA-1]
	typeB := b[lenB-1]

	if typeA > typeB {
		return -1
	} else if typeA < typeB {
		return 1
	}

	return 0
}
