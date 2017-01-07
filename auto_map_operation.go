package goriak

// This code has been extracted from riak-go-client so that we (goriak) can look at the map operations
// before executing them.
//
// https://github.com/basho/riak-go-client/blob/master/crdt_commands.go#L936:L1118
// Copyright Basho
// This file is licensed under Apache License 2.0

// MapOperation contains the instructions to send to Riak what updates to the Map you want to complete
type riakMapOperation struct {
	incrementCounters map[string]int64
	removeCounters    map[string]bool

	addToSets      map[string][][]byte
	removeFromSets map[string][][]byte
	removeSets     map[string]bool

	registersToSet  map[string][]byte
	removeRegisters map[string]bool

	flagsToSet  map[string]bool
	removeFlags map[string]bool

	maps       map[string]*riakMapOperation
	removeMaps map[string]bool
}

// IncrementCounter increments a child counter CRDT of the map at the specified key
func (mapOp *riakMapOperation) IncrementCounter(key string, increment int64) *riakMapOperation {
	if mapOp.removeCounters != nil {
		delete(mapOp.removeCounters, key)
	}
	if mapOp.incrementCounters == nil {
		mapOp.incrementCounters = make(map[string]int64)
	}
	mapOp.incrementCounters[key] += increment
	return mapOp
}

// RemoveCounter removes a child counter CRDT from the map at the specified key
func (mapOp *riakMapOperation) RemoveCounter(key string) *riakMapOperation {
	if mapOp.incrementCounters != nil {
		delete(mapOp.incrementCounters, key)
	}
	if mapOp.removeCounters == nil {
		mapOp.removeCounters = make(map[string]bool)
	}
	mapOp.removeCounters[key] = true
	return mapOp
}

// AddToSet adds an element to the child set CRDT of the map at the specified key
func (mapOp *riakMapOperation) AddToSet(key string, value []byte) *riakMapOperation {
	if mapOp.removeSets != nil {
		delete(mapOp.removeSets, key)
	}
	if mapOp.addToSets == nil {
		mapOp.addToSets = make(map[string][][]byte)
	}
	mapOp.addToSets[key] = append(mapOp.addToSets[key], value)
	return mapOp
}

// RemoveFromSet removes elements from the child set CRDT of the map at the specified key
func (mapOp *riakMapOperation) RemoveFromSet(key string, value []byte) *riakMapOperation {
	if mapOp.removeSets != nil {
		delete(mapOp.removeSets, key)
	}
	if mapOp.removeFromSets == nil {
		mapOp.removeFromSets = make(map[string][][]byte)
	}
	mapOp.removeFromSets[key] = append(mapOp.removeFromSets[key], value)
	return mapOp
}

// RemoveSet removes the child set CRDT from the map
func (mapOp *riakMapOperation) RemoveSet(key string) *riakMapOperation {
	if mapOp.addToSets != nil {
		delete(mapOp.addToSets, key)
	}
	if mapOp.removeFromSets != nil {
		delete(mapOp.removeFromSets, key)
	}
	if mapOp.removeSets == nil {
		mapOp.removeSets = make(map[string]bool)
	}
	mapOp.removeSets[key] = true
	return mapOp
}

// SetRegister sets a register CRDT on the map with the provided value
func (mapOp *riakMapOperation) SetRegister(key string, value []byte) *riakMapOperation {
	if mapOp.removeRegisters != nil {
		delete(mapOp.removeRegisters, key)
	}
	if mapOp.registersToSet == nil {
		mapOp.registersToSet = make(map[string][]byte)
	}
	mapOp.registersToSet[key] = value
	return mapOp
}

// RemoveRegister removes a register CRDT from the map
func (mapOp *riakMapOperation) RemoveRegister(key string) *riakMapOperation {
	if mapOp.registersToSet != nil {
		delete(mapOp.registersToSet, key)
	}
	if mapOp.removeRegisters == nil {
		mapOp.removeRegisters = make(map[string]bool)
	}
	mapOp.removeRegisters[key] = true
	return mapOp
}

// SetFlag sets a flag CRDT on the map
func (mapOp *riakMapOperation) SetFlag(key string, value bool) *riakMapOperation {
	if mapOp.removeFlags != nil {
		delete(mapOp.removeFlags, key)
	}
	if mapOp.flagsToSet == nil {
		mapOp.flagsToSet = make(map[string]bool)
	}
	mapOp.flagsToSet[key] = value
	return mapOp
}

// RemoveFlag removes a flag CRDT from the map
func (mapOp *riakMapOperation) RemoveFlag(key string) *riakMapOperation {
	if mapOp.flagsToSet != nil {
		delete(mapOp.flagsToSet, key)
	}
	if mapOp.removeFlags == nil {
		mapOp.removeFlags = make(map[string]bool)
	}
	mapOp.removeFlags[key] = true
	return mapOp
}

// Map returns a nested map operation for manipulation
func (mapOp *riakMapOperation) Map(key string) *riakMapOperation {
	if mapOp.removeMaps != nil {
		delete(mapOp.removeMaps, key)
	}
	if mapOp.maps == nil {
		mapOp.maps = make(map[string]*riakMapOperation)
	}

	innerMapOp, ok := mapOp.maps[key]
	if ok {
		return innerMapOp
	}

	innerMapOp = &riakMapOperation{}
	mapOp.maps[key] = innerMapOp
	return innerMapOp
}

// RemoveMap removes a nested map from the map
func (mapOp *riakMapOperation) RemoveMap(key string) *riakMapOperation {
	if mapOp.maps != nil {
		delete(mapOp.maps, key)
	}
	if mapOp.removeMaps == nil {
		mapOp.removeMaps = make(map[string]bool)
	}
	mapOp.removeMaps[key] = true
	return mapOp
}

func (mapOp *riakMapOperation) hasRemoves(includeRemoveFromSets bool) bool {
	nestedHaveRemoves := false
	for _, m := range mapOp.maps {
		if m.hasRemoves(false) {
			nestedHaveRemoves = true
			break
		}
	}

	rv := nestedHaveRemoves ||
		len(mapOp.removeCounters) > 0 ||
		len(mapOp.removeSets) > 0 ||
		len(mapOp.removeRegisters) > 0 ||
		len(mapOp.removeFlags) > 0 ||
		len(mapOp.removeMaps) > 0

	if includeRemoveFromSets {
		rv = rv || len(mapOp.removeFromSets) > 0
	}

	return rv
}
