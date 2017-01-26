package goriak

import (
	"encoding/json"
	"log"
	"testing"
)

func TestSiblings(t *testing.T) {
	key := randomKey()

	a1 := Bucket("sibs", "tests").SetJSON("bob").Key(key)
	a2 := Bucket("sibs", "tests").SetJSON("sven").Key(key)

	c := con()

	_, err := a1.Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = a2.Run(c)

	if err != nil {
		t.Error(err)
	}

	didConflictResolution := false

	resolver := func(objects []ConflictObject) ResolvedConflict {

		if len(objects) != 2 {
			t.Errorf("Did not receive 2 objects to conflict resolution. Got %d", len(objects))
		}

		for _, obj := range objects {
			if string(obj.Value) == `"bob"` {
				didConflictResolution = true
				return obj.GetResolved()
			}
		}

		return objects[0].GetResolved()
	}

	var out string
	_, err = Bucket("sibs", "tests").
		GetJSON(key, &out).
		ConflictResolver(resolver).
		Run(c)

	if err != nil {
		t.Error(err)
	}

	if !didConflictResolution {
		t.Error("Did not do conflict resolution")
	}

	didConflictResolution = false

	_, err = Bucket("sibs", "tests").
		GetJSON(key, &out).
		ConflictResolver(resolver).
		Run(c)

	if err != nil {
		t.Error(err)
	}

	if didConflictResolution {
		t.Error("Did resolution after already being resolved?")
	}
}

type ourTypeWithResolveInterface struct {
	Score int
}

func (o ourTypeWithResolveInterface) ConflictResolver(objs []ConflictObject) ResolvedConflict {
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

	return highObj.GetResolved()
}

var didInterfaceResolver bool

func TestConflictResolverInterface(t *testing.T) {
	key := randomKey()

	c := con()

	_, err := Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{200}).Key(key).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{500}).Key(key).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{400}).Key(key).Run(c)

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{300}).Key(key).Run(c)

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

	if err != nil {
		t.Error(err)
	}

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

	_, err := Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{200}).Key(key).Run(c)

	if err != nil {
		t.Error(err)
	}

	var val ourTypeWithResolveInterface
	res, err := Bucket("sibs", "tests").GetJSON(key, &val).Run(c)

	_, err = Bucket("sibs", "tests").SetJSON(ourTypeWithResolveInterface{200}).WithContext(res.Context).Key(key).Run(c)

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

func ExampleConflictResolver() {

	// For this to work you need to activate allow_mult on your bucket type
	// http://docs.basho.com/riak/kv/2.2.0/developing/usage/conflict-resolution/

	session, _ := Connect(ConnectOpts{
		Address: "127.0.0.1",
	})

	key := "object-1"

	// Save the same object without using .VClock() causing a conflict
	_, err := Bucket("bucket", "tests").SetJSON("hello").Key(key).Run(session)

	if err != nil {
		log.Println(err)
	}

	_, err = Bucket("bucket", "tests").SetJSON("worlds of conflicts!").Key(key).Run(session)

	if err != nil {
		log.Println(err)
	}

	// Our conflict resolver object
	resolver := func(objs []ConflictObject) ResolvedConflict {
		// Decide how to pick the result. We'll use len() to pick the longest value
		var maxObject ConflictObject
		var maxValue int

		for _, o := range objs {
			if len(o.Value) > maxValue {
				maxObject = o
				maxValue = len(o.Value)
			}
		}

		// Convert directly to a ResolvedConflict object
		return maxObject.GetResolved()
	}

	// Get your object
	var res string
	_, err = Bucket("bucket", "tests").
		GetJSON(key, &res).
		ConflictResolver(resolver).
		Run(session)

	if err != nil {
		log.Println(err)
	}

	// res will now contain the longest value
	log.Println(res)
}
