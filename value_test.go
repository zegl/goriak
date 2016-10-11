package goriak

import (
	"log"
	"os"
	"testing"

	"math/rand"
	"time"
)

// Cleanup Bucket
func TestMain(m *testing.M) {
	deleteAllIn("testsuite", "tests")
	deleteAllIn("customtype", "maps")
	deleteAllIn("testsuitemap", "maps")

	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func deleteAllIn(bucket, bucketType string) {
	con, _ := NewGoriak("127.0.0.1")
	keys, _ := con.AllKeys(bucket, bucketType)

	for _, key := range keys {
		log.Println("Delete:", key)
		con.Delete(bucket, bucketType, key)
	}
}

type teststoreobject struct {
	A string `goriakindex:"testindex_bin"`
	B int
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomKey() string {
	b := make([]rune, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func TestGetSetValue(t *testing.T) {
	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetValue("testsuite", "tests", key, teststoreobject{
		A: "Abc",
		B: 10002,
	})

	if err != nil {
		t.Error("SetValue:", err)
		return
	}

	var res teststoreobject
	getErr := con.GetValue("testsuite", "tests", key, &res)

	if getErr != nil {
		t.Error("GetValue:", getErr)
		t.Errorf("%+v", res)
		return
	}

	if res.A != "Abc" {
		t.Error("Wrong A value")
	}

	if res.B != 10002 {
		t.Error("Wrong B value")
	}
}

func TestValueWithIndex(t *testing.T) {
	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetValue("testsuite", "tests", key, teststoreobject{
		A: "HelloWorld",
		B: 10002,
	})

	if err != nil {
		t.Error("SetValue:", err)
		return
	}

	keys, err := con.KeysInIndex("testsuite", "tests", "testindex_bin", "HelloWorld", 100)

	if len(keys) != 1 {
		t.Error("Did not receive exactly 1 key")
		t.Error(keys)
		return
	}

	if keys[0] != key {
		t.Error("The wrong key was returned")
		return
	}
}

type testsliceindex struct {
	Thing   string
	Indexes []string `goriakindex:"sliceindex_bin"`
}

func TestValueWithSliceIndex(t *testing.T) {
	key := randomKey()
	con, _ := NewGoriak("127.0.0.1")

	err := con.SetValue("testsuite", "tests", key, testsliceindex{
		Thing:   "Hello",
		Indexes: []string{"Hola", "Hej", "Halo"},
	})

	if err != nil {
		t.Error(err)
	}

	keys, err := con.KeysInIndex("testsuite", "tests", "sliceindex_bin", "Hej", 100)

	if err != nil {
		t.Error(err)
	}

	if len(keys) != 1 || keys[0] != key {
		t.Error("1: Unexpected content")
		t.Errorf("%+v", keys)
	}

	keys, err = con.KeysInIndex("testsuite", "tests", "sliceindex_bin", "Hola", 100)

	if err != nil {
		t.Error(err)
	}

	if len(keys) != 1 || keys[0] != key {
		t.Error("2: Unexpected content")
		t.Errorf("%+v", keys)
	}
}
