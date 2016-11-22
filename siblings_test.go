package goriak

import (
	"math/rand"
	"testing"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomKey() string {
	n := 10
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

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
