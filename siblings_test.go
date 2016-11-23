package goriak

import (
	"encoding/json"
	"testing"
)

func TestSiblings(t *testing.T) {
	key := randomKey()

	a1 := Bucket("sibs", "tests").Key(key).SetJSON("bob")
	a2 := Bucket("sibs", "tests").Key(key).SetJSON("sven")

	c := con()

	_, err := a1.Run(c)

	if err != nil {
		t.Error()
	}

	_, err = a2.Run(c)

	if err != nil {
		t.Error()
	}

	didConflictResolution := false

	resolver := func(objects []ConflictObject) ConflictObject {
		//t.Logf("In conflict resolution: %+v", objects)

		if len(objects) != 2 {
			t.Errorf("Did not receive 2 objects to conflict resolution. Got %d", len(objects))
		}

		for _, obj := range objects {
			if string(obj.Value) == `"bob"` {
				didConflictResolution = true
				return obj
			}
		}

		return objects[0]
	}

	var out string
	_, err = Bucket("sibs", "tests").
		ConflictResolver(resolver).
		GetJSON(key, &out).
		Run(c)

	if err != nil {
		t.Error()
	}

	if !didConflictResolution {
		t.Error("Did not do conflict resolution")
	}

	didConflictResolution = false

	_, err = Bucket("sibs", "tests").
		ConflictResolver(resolver).
		GetJSON(key, &out).
		Run(c)

	if didConflictResolution {
		t.Error("Did resolution after already beeing resolved?")
	}
}

type ourTypeWithResolveInterface struct {
	Score int
}

func (o ourTypeWithResolveInterface) ConflictResolver(objs []ConflictObject) ConflictObject {
	var highObj ConflictObject
	var highScore int

	didInterfaceResolver = true

	for _, o := range objs {

		var val ourTypeWithResolveInterface
		err := json.Unmarshal(o.Value, &val)

		if err == nil {
			if val.Score > highScore {

				highScore = val.Score
				highObj = o
			}
		}
	}

	return highObj
}

var didInterfaceResolver bool

func TestConflictResolverInterface(t *testing.T) {
	key := randomKey()

	c := con()

	_, err := Bucket("sibs", "tests").Key(key).SetJSON(ourTypeWithResolveInterface{200}).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").Key(key).SetJSON(ourTypeWithResolveInterface{500}).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").Key(key).SetJSON(ourTypeWithResolveInterface{400}).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").Key(key).SetJSON(ourTypeWithResolveInterface{300}).Run(c)

	if err != nil {
		t.Error(err)
	}

	var val ourTypeWithResolveInterface
	_, err = Bucket("sibs", "tests").GetJSON(key, &val).Run(c)

	if err != nil {
		t.Error(err)
	}

	if val.Score != 500 {
		t.Error("Did not get the item with the highest score back")
	}

	if !didInterfaceResolver {
		t.Error("Did not do interface resolver")
	}

	// Test again
	didInterfaceResolver = false
	var val2 ourTypeWithResolveInterface
	_, err = Bucket("sibs", "tests").GetJSON(key, &val2).Run(c)

	if didInterfaceResolver {
		t.Error("Used resolver even if not needed")
	}

	if val.Score != 500 {
		t.Error("Did not get the item with the highest score back second time")
	}
}

func TestPreventConflicts(t *testing.T) {
	key := randomKey()
	c := con()

	_, err := Bucket("sibs", "tests").Key(key).SetJSON(ourTypeWithResolveInterface{200}).Run(c)

	if err != nil {
		t.Error(err)
	}

	var val ourTypeWithResolveInterface
	res, err := Bucket("sibs", "tests").GetJSON(key, &val).Run(c)

	_, err = Bucket("sibs", "tests").VClock(res.VClock).Key(key).SetJSON(ourTypeWithResolveInterface{200}).Run(c)

	if err != nil {
		t.Error(err)
	}

	var val2 ourTypeWithResolveInterface
	res, err = Bucket("sibs", "tests").GetJSON(key, &val2).Run(c)

	if err != nil {
		t.Error(err)
	}

	if didInterfaceResolver {
		t.Error("Had to do interface resolver even with VClock set")
	}
}
