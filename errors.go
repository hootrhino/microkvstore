package mkvstore

import "errors"

var (
	// ErrKeyNotFound is returned when a key does not exist or is expired.
	ErrKeyNotFound = errors.New("key not found or expired")

	// ErrWrongType is returned when the key exists but is not a string type.
	// (Future use if we add other types)
	ErrWrongType = errors.New("operation against a key holding the wrong kind of value")
)
