package goriak

import (
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	type ourTestType struct {
		TS time.Time
	}

	val := ourTestType{
		TS: time.Now(),
	}

	c := con()

	res, err := bucket().Set(&val).Run(c)

	if err != nil {
		t.Error("Set:", err.Error())
	}

	var fetch ourTestType
	res, err = bucket().Get(res.Key, &fetch).Run(c)

	if err != nil {
		t.Error("Fetch:", err.Error())
	}

	if val.TS.UnixNano() != fetch.TS.UnixNano() {
		t.Error("Did not get same value back")
		t.Logf("%+v", val.TS)
		t.Logf("%+v", fetch.TS)
		t.Logf("%+v", val.TS.UnixNano())
		t.Logf("%+v", fetch.TS.UnixNano())
	}
}
