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
