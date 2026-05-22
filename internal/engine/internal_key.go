package engine

import (
	"bytes"
	"encoding/binary"
)

const (
	TypeDelete byte = 0
	TypePut    byte = 1
)

const internalKeySuffixLen = 9

func EncodeInternalKey(userKey []byte, seqNum uint64, keyType byte) []byte {
	size := len(userKey) + internalKeySuffixLen
	buf := make([]byte, size)

	copy(buf, userKey)

	binary.BigEndian.PutUint64(buf[len(userKey):], seqNum)

	buf[size-1] = keyType

	return buf
}

func ParseInternalKey(internalKey []byte) (userKey []byte, seqNum uint64, keyType byte) {
	if len(internalKey) < internalKeySuffixLen {
		return nil, 0, 0
	}

	suffixStart := len(internalKey) - internalKeySuffixLen

	userKey = internalKey[:suffixStart]
	seqNum = binary.BigEndian.Uint64(internalKey[suffixStart : suffixStart+8])
	keyType = internalKey[len(internalKey)-1]

	return userKey, seqNum, keyType
}

func CompareInternalKeys(a, b []byte) int {
	lenA := len(a)
	lenB := len(b)

	if lenA < internalKeySuffixLen || lenB < internalKeySuffixLen {
		return 0
	}

	userKeyLenA := lenA - internalKeySuffixLen
	userKeyLenB := lenB - internalKeySuffixLen

	cmp := bytes.Compare(a[:userKeyLenA], b[:userKeyLenB])
	if cmp != 0 {
		return cmp
	}

	seqNumA := binary.BigEndian.Uint64(a[userKeyLenA : userKeyLenA+8])
	seqNumB := binary.BigEndian.Uint64(b[userKeyLenB : userKeyLenB+8])

	if seqNumA > seqNumB {
		return -1
	} else if seqNumA < seqNumB {
		return 1
	}

	typeA := a[lenA-1]
	typeB := b[lenB-1]

	if typeA > typeB {
		return -1
	} else if typeA < typeB {
		return 1
	}

	return 0
}
