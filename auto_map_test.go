package goriak

import (
	"reflect"
	"testing"
)

type testmapobject struct {
	A   string
	Set []string
}

func TestAutoMapSetAndGet(t *testing.T) {
	key := randomKey()

	con, _ := NewGoriak("127.0.0.1")

	err := con.Delete("testsuitemap", "maps", key)

	if err != nil {
		t.Error("Could not delete: " + err.Error())
	}

	errset := con.SetMap("testsuitemap", "maps", key, &testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	})

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject
	errget, isNotFound := con.GetMap("testsuitemap", "maps", key, &res)

	if errget != nil {
		t.Error("Set:", errset)
	}

	if isNotFound {
		t.Error("Not found")
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
	key := randomKey()

	con, _ := NewGoriak("127.0.0.1")
	err := con.Delete("testsuitemap", "maps", key)

	if err != nil {
		t.Error("Could not delete: " + err.Error())
	}

	errset := con.SetMap("testsuitemap", "maps", key, &testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	})

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject
	con.GetMap("testsuitemap", "maps", key, &res)

	if len(res.Set) != 2 {
		t.Error("Unexpected length. Should be 2, got ", len(res.Set))
	}

	op := NewMapOperation()
	op.AddToSet("Set", []byte("Three"))

	mapoperr := con.MapOperation("testsuitemap", "maps", key, op)

	if mapoperr != nil {
		t.Error("MapOperr:", mapoperr)
	}

	var res2 testmapobject
	errget, _ := con.GetMap("testsuitemap", "maps", key, &res2)

	if errget != nil {
		t.Error("ErrGet:", errget)
	}

	if len(res2.Set) != 3 {
		t.Error("Unexpected length. Should be 3, got ", len(res2.Set))
		t.Errorf("%+v", res2)
	}
}

func TestIsNotFound(t *testing.T) {
	con, _ := NewGoriak("127.0.0.1")

	var res testmapobject
	err, isNotFound := con.GetMap("testsuitemap", "maps", randomKey(), &res)

	if !isNotFound {
		t.Error("not marked as not found")
	}

	if err == nil {
		t.Error("did not get error")
	}
}

func TestSetNonPointer(t *testing.T) {
	input := testmapobject{
		A: "I am passed as Value",
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, input)

	if err != nil {
		t.Error("Error: ", err.Error())
	}

	var res testmapobject
	err, isNotFound := con.GetMap("testsuitemap", "maps", key, &res)

	if isNotFound {
		t.Error("Not found")
	}

	if err != nil {
		t.Error(err)
	}

	if res.A != "I am passed as Value" {
		t.Error("Unkown response")
	}
}

type aBunchOfTypes struct {
	Int    int
	String string
	Array  [3]byte
	Slice  []byte

	StringSlice []string `goriak:"callme_string_slicer"`
	IntSlice    []int
}

func TestAbunchOfTypes(t *testing.T) {
	o := aBunchOfTypes{
		Int:         9001,
		String:      "Hello World",
		Array:       [3]byte{100, 101, 102},
		Slice:       []byte{50, 60, 70},
		StringSlice: []string{"H", "e", "l", "o"},
		IntSlice:    []int{4000, 5000, 6000},
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err != nil {
		t.Error("Set", err)
	}

	var res aBunchOfTypes
	err, isNotFound := con.GetMap("testsuitemap", "maps", key, &res)

	if err != nil {
		t.Error("Get", err)
	}

	if isNotFound {
		t.Error("Not found")
	}

	if !reflect.DeepEqual(o, res) {
		t.Error("Not equal")
		t.Errorf("Got: %+v", res)
		t.Errorf("Expected: %+v", o)
	}

}

func TestFailNonMapType(t *testing.T) {
	con, _ := NewGoriak("127.0.0.1")
	err := con.SetMap("testsuitemap", "maps", randomKey(), 500)

	if err == nil {
		t.Error("Did not receive error")
	}
}

func TestFailEmptyArray(t *testing.T) {
	type testType struct {
		A [0]byte
	}

	o := testType{}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err != nil {
		t.Error(err)
	}

	var res testType
	getErr, isNotFound := con.GetMap("testsuitemap", "maps", key, &res)

	if isNotFound {
		t.Error("not found")
	}

	if getErr != nil {
		t.Error(getErr)
	}
}

func TestUnsupportedArrayType(t *testing.T) {
	type testType struct {
		A [5]string
	}

	o := testType{}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err == nil {
		t.Error("Did not get error")
	}

	if err.Error() != "Unkown Array type: string" {
		t.Error("Unkown error")
		t.Error(err)
	}
}

func TestUnsupportedSliceType(t *testing.T) {
	type testType struct {
		A []bool
	}

	o := testType{
		A: []bool{false, true, true, true, false, true},
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err == nil {
		t.Error("Did not get error")
		return
	}

	if err.Error() != "Unknown slice type: bool" {
		t.Error("Unkown error")
		t.Error(err)
	}
}

/*func TestUnsupportedType(t *testing.T) {
	type testType struct {
		A bool
	}

	o := testType{
		A: true,
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err == nil {
		t.Error("Did not get error")
		return
	}

	if err.Error() != "Unexpected type: bool" {
		t.Error("Unkown error")
		t.Error(err)
	}
}*/

func TestMapBool(t *testing.T) {
	type testType struct {
		A bool
		B bool
	}

	o := testType{
		A: true,
		B: false,
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetMap("testsuitemap", "maps", key, o)

	if err != nil {
		t.Error(err)
		return
	}

	var res testType
	err, isNotFound := con.GetMap("testsuitemap", "maps", key, &res)

	if err != nil {
		t.Error(err)
	}

	if isNotFound {
		t.Error("Not Found")
	}

	if !res.A {
		t.Error("A was not true")
	}

	if res.B {
		t.Error("B was not false")
	}
}
