package goriak

import (
	"testing"
)

func TestKeysInIndexRange(t *testing.T) {
	type tt struct {
		Val string
	}

	for _, key := range []string{"A", "B", "C", "D", "E", "F", "G"} {
		_, err := Bucket("json", "default").SetJSON(tt{Val: key}).AddToIndex("rangetest_bin", "AAAA").Key(key).Run(con())
		if err != nil {
			t.Error(err)
		}
	}

	fetches := 0
	keyCount := 0

	cb := func(r SecondaryIndexQueryResult) {
		if !r.IsComplete {
			keyCount++
		}
	}

	var cont []byte

	for {
		res, err := Bucket("json", "default").
			KeysInIndex("rangetest_bin", "AAAA", cb).
			Limit(3).
			IndexContinuation(cont).
			Run(con())

		if err != nil {
			t.Error(err)
		}

		fetches++

		if len(res.Continuation) == 0 {
			break
		}

		cont = res.Continuation
	}

	if fetches != 3 {
		t.Error("Did not do 3 fetches")
	}

	if keyCount != 7 {
		t.Error("did not find 7 keys")
	}
}
