package oura

import (
	"encoding/binary"
	"errors"
)

var (
	ErrDBClosed    = errors.New("DB is closed")
	ErrEmptyQueue  = errors.New("queue is empty")
	ErrKeyNotFound = errors.New("key not found")
)

// intToKey converts a uint64 to a valid DB key.
func intToKey(i uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, i)
	return key
}

// keyToInt converts a key to uint64.
func keyToInt(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}
