package goriak

// This code has been extracted from riak-go-client so that we (goriak) can look at the map operations
// before executing them.
//
// Parts of the original code has been removed.
//
// https://github.com/basho/riak-go-client/blob/master/crdt_commands.go#L936:L1118

// MapOperation contains the instructions to send to Riak what updates to the Map you want to complete
type riakMapOperation struct {
	incrementCounters map[string]int64

	addToSets      map[string][][]byte
	removeFromSets map[string][][]byte

	registersToSet map[string][]byte

	flagsToSet map[string]bool

	maps map[string]*riakMapOperation
}

// IncrementCounter increments a child counter CRDT of the map at the specified key
func (mapOp *riakMapOperation) IncrementCounter(key string, increment int64) *riakMapOperation {
	if mapOp.incrementCounters == nil {
		mapOp.incrementCounters = make(map[string]int64)
	}
	mapOp.incrementCounters[key] += increment
	return mapOp
}

// AddToSet adds an element to the child set CRDT of the map at the specified key
func (mapOp *riakMapOperation) AddToSet(key string, value []byte) *riakMapOperation {

	if mapOp.addToSets == nil {
		mapOp.addToSets = make(map[string][][]byte)
	}
	mapOp.addToSets[key] = append(mapOp.addToSets[key], value)
	return mapOp
}

// RemoveFromSet removes elements from the child set CRDT of the map at the specified key
func (mapOp *riakMapOperation) RemoveFromSet(key string, value []byte) *riakMapOperation {

	if mapOp.removeFromSets == nil {
		mapOp.removeFromSets = make(map[string][][]byte)
	}
	mapOp.removeFromSets[key] = append(mapOp.removeFromSets[key], value)
	return mapOp
}

// SetRegister sets a register CRDT on the map with the provided value
func (mapOp *riakMapOperation) SetRegister(key string, value []byte) *riakMapOperation {
	if mapOp.registersToSet == nil {
		mapOp.registersToSet = make(map[string][]byte)
	}
	mapOp.registersToSet[key] = value
	return mapOp
}

// SetFlag sets a flag CRDT on the map
func (mapOp *riakMapOperation) SetFlag(key string, value bool) *riakMapOperation {
	if mapOp.flagsToSet == nil {
		mapOp.flagsToSet = make(map[string]bool)
	}
	mapOp.flagsToSet[key] = value
	return mapOp
}

// Map returns a nested map operation for manipulation
func (mapOp *riakMapOperation) Map(key string) *riakMapOperation {
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
