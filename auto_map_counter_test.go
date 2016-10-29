package goriak

import (
	"testing"
)

func TestMapCounter(t *testing.T) {

	type testType struct {
		Foos *Counter
	}

	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	testVal := testType{
		Foos: NewCounter(),
	}

	errset := con.SetMap("testsuitemap", "maps", key, &testVal)

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testType
	errget, _ := con.GetMap("testsuitemap", "maps", key, &res)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res.Foos.Value() != 0 {
		t.Error("Unexpected initial value")
	}

	// Increase by one
	err := res.Foos.Increase(1).Exec(con)

	if err != nil {
		t.Error("Error Increase: ", err.Error())
	}

	if res.Foos.Value() != 1 {
		t.Error("a: Unexpected value:", res.Foos.Value())
	}

	// Get from Raik
	var res2 testType
	errget, _ = con.GetMap("testsuitemap", "maps", key, &res2)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res2.Foos.Value() != 1 {
		t.Error("b: Unexpected value:", res2.Foos.Value())
	}

	err = res2.Foos.Increase(3).Exec(con)

	if err != nil {
		t.Error("Error Increase: ", err.Error())
	}

	// Get from Raik
	var res3 testType
	errget, _ = con.GetMap("testsuitemap", "maps", key, &res3)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res3.Foos.Value() != 4 {
		t.Error("c: Unexpected value:", res3.Foos.Value())
	}
}

func TestMapCounterError(t *testing.T) {
	type testType struct {
		Foos *Counter
	}

	con, _ := NewGoriak("127.0.0.1")

	testVal := testType{
		Foos: NewCounter(),
	}

	err := testVal.Foos.Increase(4).Exec(con)

	if err == nil {
		t.Error("No error")
	}

	if err != nil && err.Error() != "Unknown path to counter. Retreive counter with GetMap before updating the counter" {
		t.Error(err)
	}
}

func TestMapCounterError2(t *testing.T) {
	type testType struct {
		Foos *Counter
	}

	con, _ := NewGoriak("127.0.0.1")

	testVal := testType{}

	err := testVal.Foos.Increase(4).Exec(con)

	if err == nil {
		t.Error("No error")
	}

	if err != nil && err.Error() != "Nil Counter" {
		t.Error(err)
	}
}
