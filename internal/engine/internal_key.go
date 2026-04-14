/**
 * internal_key.go : Responsible for comparison, encoding and parsing of the Key
 * as we aim to hit consitency over format of key throughout.
 * There is no other use, just this simple usage, basically here for modularity
 */
package engine

import (
	"bytes"
	"encoding/binary"
)

// We can only have Delete or Put operation, not modify
const (
	TypeDelete byte = 0
	TypePut    byte = 1
)

const internalKeySuffixLen = 9 // 8 bytes for SeqNum(Time Stamp) + 1 byte for Type of Operation

// EncodeInternalKey packs a user key, a sequence number, and an operation type into a single byte slice.
// Format: [UserKey (Variable)] + [SeqNum (8 Bytes)] + [Type (1 Byte)]
func EncodeInternalKey(userKey []byte, seqNum uint64, keyType byte) []byte {
	size := len(userKey) + internalKeySuffixLen
	buf := make([]byte, size)

	copy(buf, userKey)

	binary.BigEndian.PutUint64(buf[len(userKey):], seqNum) // Why Big Endian ? For easy lexicographical sorting

	buf[size-1] = keyType

	return buf
}

// ParseInternalKey extracts the components of an InternalKey WITHOUT allocating new memory.
// It returns a slice pointing to the original UserKey bytes.
func ParseInternalKey(internalKey []byte) (userKey []byte, seqNum uint64, keyType byte) {
	if len(internalKey) < internalKeySuffixLen {
		return nil, 0, 0 // Invalid key
	}

	suffixStart := len(internalKey) - internalKeySuffixLen // This is the length of the Key, because suffixLen is already 9

	userKey = internalKey[:suffixStart]
	seqNum = binary.BigEndian.Uint64(internalKey[suffixStart : suffixStart+8])
	keyType = internalKey[len(internalKey)-1]

	return userKey, seqNum, keyType
}

// CompareInternalKeys is the heart of our MVCC sorting.
// 1: Sort by UserKey Ascending (A-Z)
// 2: If UserKeys match, sort by SeqNum DESCENDING (Newest first) - This is used when evicting keys,out of range
func CompareInternalKeys(a, b []byte) int {
	lenA := len(a)
	lenB := len(b)

	if lenA < internalKeySuffixLen || lenB < internalKeySuffixLen {
		// none of the length can be less than internakKeySuffixLen, if it is, then it was partial written
		return 0
	}

	userKeyLenA := lenA - internalKeySuffixLen
	userKeyLenB := lenB - internalKeySuffixLen

	// Yeah, comparing the keys only
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
