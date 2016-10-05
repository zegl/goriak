package goriak

import (
	"testing"
)

type testmapobject struct {
	A   string
	Set []string
}

func TestAutoMapSetAndGet(t *testing.T) {
	err := Delete("testsuitemap", "maps", "testMap1")

	if err != nil {
		t.Error("Could not delete: " + err.Error())
	}

	errset := SetMap("testsuitemap", "maps", "testMap1", &testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	})

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject
	errget := GetMap("testsuitemap", "maps", "testMap1", &res)

	if errget != nil {
		t.Error("Set:", errset)
	}

	if res.A != "Hello" {
		t.Error("Unexpected A value")
	}

	if len(res.Set) != 2 {
		t.Error("Unexpected set length")
	}

	foundOne := false
	foundTwo := false

	for _, v := range res.Set {
		if v == "One" {
			foundOne = true
		}

		if v == "Two" {
			foundTwo = true
		}
	}

	if !foundOne || !foundTwo {
		t.Error("Unexpected set contents")
	}
}

func TestMapOperation(t *testing.T) {
	err := Delete("testsuitemap", "maps", "testMap2")

	if err != nil {
		t.Error("Could not delete: " + err.Error())
	}

	errset := SetMap("testsuitemap", "maps", "testMap2", &testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	})

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject
	GetMap("testsuitemap", "maps", "testMap2", &res)

	if len(res.Set) != 2 {
		t.Error("Unexpected length. Should be 2, got ", len(res.Set))
	}

	op := NewMapOperation()
	op.AddToSet("Set", []byte("Three"))

	mapoperr := MapOperation("testsuitemap", "maps", "testMap2", op)

	if mapoperr != nil {
		t.Error("MapOperr:", mapoperr)
	}

	var res2 testmapobject
	errget := GetMap("testsuitemap", "maps", "testMap2", &res2)

	if errget != nil {
		t.Error("ErrGet:", errget)
	}

	if len(res2.Set) != 3 {
		t.Error("Unexpected length. Should be 3, got ", len(res2.Set))
		t.Errorf("%+v", res2)
	}
}
