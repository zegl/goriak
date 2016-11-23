package goriak

import (
	"time"
)

type ConflictObject struct {
	Value        []byte
	VClock       []byte
	LastModified time.Time
}

type ConflictResolver interface {
	ConflictResolver([]ConflictObject) ConflictObject
}
