package goriak

import (
	"time"
)

type ConflictObject struct {
	Value        []byte
	VClock       []byte
	LastModified time.Time
}

type ResolvedConflict struct {
	Value  []byte
	VClock []byte
}

// GetResolved creates a ResolvedConflict object
func (r ConflictObject) GetResolved() ResolvedConflict {
	return ResolvedConflict{
		Value:  r.Value,
		VClock: r.VClock,
	}
}

// The ConflictResolver interface is used to solve conflicts when using Get() and GetJSON().
// All versions will be sent to your ConflictResolver method. Return the (merged) version that you want to keep.
type ConflictResolver interface {
	ConflictResolver([]ConflictObject) ResolvedConflict
}
