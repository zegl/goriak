package goriak

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Cleanup Bucket
func TestMain(m *testing.M) {
	deleteAllIn("testsuite", "tests")
	deleteAllIn("customtype", "maps")
	deleteAllIn("testsuitemap", "maps")
	deleteAllIn("json", "default")
	deleteAllIn("testdelete", "default")

	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

func deleteAllIn(bucket, bucketType string) {

	cb := func(res []string) error {

		for _, key := range res {
			Bucket(bucket, bucketType).Delete(key).Run(con())
			log.Println("Delete: " + key)
		}

		return nil
	}

	Bucket(bucket, bucketType).AllKeys(cb).Run(con())
}

func TestAllKeys(t *testing.T) {

	c := con()

	Bucket("testdelete", "default").SetRaw([]byte{1, 2, 3, 4}).Run(c)
	Bucket("testdelete", "default").SetRaw([]byte{1, 2, 3, 4}).Run(c)
	Bucket("testdelete", "default").SetRaw([]byte{1, 2, 3, 4}).Run(c)
	Bucket("testdelete", "default").SetRaw([]byte{1, 2, 3, 4}).Run(c)

	foundCount := 0

	_, err := Bucket("testdelete", "default").AllKeys(func(res []string) error {
		foundCount = foundCount + len(res)
		return nil
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	if foundCount == 0 {
		t.Error("No keys found")
	}
}

func TestAllKeysNoBucket(t *testing.T) {
	cmd := &Command{}
	_, err := cmd.AllKeys(func(res []string) error {
		return nil
	}).Run(con())

	if err.Error() != "ClientError|Bucket is required" {
		t.Error("Unexpected error")
	}
}

func TestDelete(t *testing.T) {
	res, err := Bucket("testdelete", "default").SetRaw([]byte{1, 2, 3, 4}).Run(con())

	if err != nil {
		t.Error(err)
	}

	_, err = Bucket("testdelete", "default").Delete(res.Key).Run(con())

	if err != nil {
		t.Error(err)
	}

	var out []byte
	res, err = Bucket("testdelete", "default").GetRaw(res.Key, &out).Run(con())

	if !res.NotFound {
		t.Error("Found after delete")
	}

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Not found" {
		t.Error("Unexpected error:", err)
	}
}

func TestDeleteNoKey(t *testing.T) {
	_, err := Bucket("testdelete", "default").Delete("").Run(con())

	if err.Error() != "ClientError|Key is required" {
		t.Error("Unexpected error")
	}
}
