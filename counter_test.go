package goriak

import (
	"testing"
)

func con() *Session {
	c, _ := Connect(ConnectOpts{
		Address: "127.0.0.1",
	})

	return c
}

func bucket() Command {

	return Bucket("testsuitemap", "maps")
}

func TestMapCounter(t *testing.T) {

	type testType struct {
		Foos *Counter
	}

	testVal := testType{
		Foos: NewCounter(),
	}

	queryRes, errset := bucket().Set(testVal).Run(con())

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testType
	queryRes, errget := bucket().Get(queryRes.Key, &res).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res.Foos.Value() != 0 {
		t.Error("Unexpected initial value")
	}

	// Increase by one
	err := res.Foos.Increase(1).Exec(con())

	if err != nil {
		t.Error("Error Increase: ", err.Error())
	}

	if res.Foos.Value() != 1 {
		t.Error("a: Unexpected value:", res.Foos.Value())
	}

	// Get from Raik
	var res2 testType
	_, errget = bucket().Get(queryRes.Key, &res2).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	if res2.Foos.Value() != 1 {
		t.Error("b: Unexpected value:", res2.Foos.Value())
	}

	err = res2.Foos.Increase(3).Exec(con())

	if err != nil {
		t.Error("Error Increase: ", err.Error())
	}

	// Get from Raik
	var res3 testType
	_, errget = bucket().Get(queryRes.Key, &res3).Run(con())

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

	testVal := testType{
		Foos: NewCounter(),
	}

	err := testVal.Foos.Increase(4).Exec(con())

	if err == nil {
		t.Error("No error")
	}

	if err != nil && err.Error() != "Unknown path to counter. Retrieve counter with GetMap before updating the counter" {
		t.Error(err)
	}
}

func TestMapCounterError2(t *testing.T) {
	type testType struct {
		Foos *Counter
	}

	testVal := testType{}

	err := testVal.Foos.Increase(4).Exec(con())

	if err == nil {
		t.Error("No error")
	}

	if err != nil && err.Error() != "Nil Counter" {
		t.Error(err)
	}
}

func TestMapCounterNestedMap(t *testing.T) {

	type subTestType struct {
		Visits *Counter
	}

	type testType struct {
		Counts subTestType
	}

	testVal := testType{
		Counts: subTestType{
			Visits: NewCounter(),
		},
	}

	result, errset := bucket().Set(testVal).Run(con())

	// errset := con.SetMap("testsuitemap", "maps", key, &testVal)

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testType
	_, errget := bucket().Get(result.Key, &res).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}

	err := res.Counts.Visits.Increase(17).Exec(con())

	if err != nil {
		t.Error("Increase:", err)
	}

	var res2 testType
	_, errget2 := bucket().Get(result.Key, &res2).Run(con())

	if errget2 != nil {
		t.Error("Get2:", errget2)
	}

	if res2.Counts.Visits.Value() != 17 {
		t.Error("Unexpected value")
	}
}

func TestNilCounter(t *testing.T) {
	type testType struct {
		Foos *Counter
	}

	testVal := testType{}

	result, errset := bucket().Set(testVal).Run(con())

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testType
	_, errget := bucket().Get(result.Key, &res).Run(con())

	if errget != nil {
		t.Error("Get:", errget)
	}
}

func TestCounterParalell(t *testing.T) {
	type testType struct {
		Foos *Counter
	}

	testVal := testType{
		Foos: NewCounter(),
	}

	res, err := bucket().Set(testVal).Run(con())

	if err != nil {
		t.Error(err)
	}

	var getVal1 testType
	_, err = bucket().Get(res.Key, &getVal1).Run(con())

	if err != nil {
		t.Error(err)
	}

	var getVal2 testType
	_, err = bucket().Get(res.Key, &getVal2).Run(con())

	if err != nil {
		t.Error(err)
	}

	err = getVal1.Foos.Increase(1).Exec(con())

	if err != nil {
		t.Error(err)
	}

	err = getVal2.Foos.Increase(1).Exec(con())

	if err != nil {
		t.Error(err)
	}

	if getVal1.Foos.Value() != 1 {
		t.Error("GetVal1 was not 1")
	}

	if getVal2.Foos.Value() != 2 {
		t.Error("GetVal2 was not 2")
	}

	err = getVal1.Foos.Increase(1).Exec(con())

	if err != nil {
		t.Error(err)
	}

	if getVal1.Foos.Value() != 3 {
		t.Error("GetVal1 was not 3")
	}
}
