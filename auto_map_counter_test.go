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
		Foos: &Counter{
			val: 20,
		},
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

	if res.Foos.Value() != 20 {
		t.Error("Unexpected initial value")
	}

	// Increase by one
	res.Foos.Increase(1)

	if res.Foos.Value() != 21 {
		t.Error("Unexpected value")
	}

	// Get from Raik

	var res2 testType
	errget, _ = con.GetMap("testsuitemap", "maps", key, &res2)

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res2.Foos.Value() != 21 {
		t.Error("Unexpected value")
	}
}
