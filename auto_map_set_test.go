package goriak

import (
	"reflect"
	"testing"
)

func TestAutoMapSet(t *testing.T) {
	type ourTestType struct {
		Items *Set
	}

	// Initialize
	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	testVal := ourTestType{}
	errset := con.SetMap("testsuitemap", "maps", key, &testVal)

	if errset != nil {
		t.Error("Set:", errset)
	}

	// Get when empty
	var res ourTestType
	errget, _ := con.GetMap("testsuitemap", "maps", key, &res)

	if errget != nil {
		t.Error("Get:", errget)
	}

	res.Items.AddString("aaa")
	res.Items.AddString("bbb")
	res.Items.AddString("ccc")

	s := res.Items.Strings()

	if len(s) != 3 {
		t.Error("Unexpected length")
	}

	expected := []string{"aaa", "bbb", "ccc"}

	if !reflect.DeepEqual(expected, res.Items.Strings()) {
		t.Log(expected)
		t.Log(res.Items.Strings())
		t.Error("Not deep equal first")
	}

	err := res.Items.Exec(con)

	if err != nil {
		t.Error("Exec1: ", err)
	}

	// Get after save
	var res2 ourTestType
	errget, _ = con.GetMap("testsuitemap", "maps", key, &res2)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if !reflect.DeepEqual(res2.Items.Value(), res.Items.Value()) {
		t.Error("Not deep equal after get")
	}

	// Remove
	err = res2.Items.RemoveString("bbb").Exec(con)

	if err != nil {
		t.Error("Exec2: ", err)
	}

	// Get after remove
	var res3 ourTestType
	errget, _ = con.GetMap("testsuitemap", "maps", key, &res3)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if !reflect.DeepEqual(res3.Items.Value(), res2.Items.Value()) {
		t.Log(res3.Items.Strings())
		t.Log(res2.Items.Strings())
		t.Error("Not deep equal after get 2")
	}
}

func TestAutoMapSetAddRemove(t *testing.T) {
	set := NewSet()

	set.AddString("1")
	set.AddString("2")
	set.AddString("3")

	expected := []string{"1", "2", "3"}

	if !reflect.DeepEqual(expected, set.Strings()) {
		t.Log(expected)
		t.Log(set.Strings())
		t.Error("Unexpected 1")
	}

	set.RemoveString("2")

	expected = []string{"1", "3"}

	if !reflect.DeepEqual(expected, set.Strings()) {
		t.Log(expected)
		t.Log(set.Strings())
		t.Error("Unexpected 2")
	}

	expectedAdds := [][]byte{[]byte("1"), []byte("3")}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Log(expectedAdds)
		t.Log(set.adds)
		t.Error("Unexpected adds 1")
	}

	expectedRemoves := [][]byte{[]byte("2")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 1")
	}

	set.RemoveString("4")

	expectedRemoves = [][]byte{[]byte("2"), []byte("4")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 2")
	}

	set.AddString("4")

	expectedAdds = [][]byte{[]byte("1"), []byte("3"), []byte("4")}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Log(expectedAdds)
		t.Log(set.adds)
		t.Error("Unexpected adds 2")
	}

	expectedRemoves = [][]byte{[]byte("2")}

	if !reflect.DeepEqual(expectedRemoves, set.removes) {
		t.Log(expectedRemoves)
		t.Log(set.removes)
		t.Error("Unexpected removes 3")
	}
}

func TestAutoMapMultipleSet(t *testing.T) {
	set := NewSet()

	set.AddString("hello")
	set.AddString("hello")
	set.AddString("hello")
	set.AddString("hello")

	expectedVal := []string{"hello"}
	expectedAdds := [][]byte{[]byte("hello")}

	if !reflect.DeepEqual(expectedVal, set.Strings()) {
		t.Error("Unexpected value")
	}

	if !reflect.DeepEqual(expectedAdds, set.adds) {
		t.Error("Unexpected adds")
	}
}
