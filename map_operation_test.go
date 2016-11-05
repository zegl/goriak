package goriak

import (
	"testing"
)

func TestMapOperation(t *testing.T) {

	result, errset := bucket().Set(&testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	}).Run(con())

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject
	bucket().Get(result.Key, &res).Run(con())

	if len(res.Set) != 2 {
		t.Error("Unexpected length. Should be 2, got ", len(res.Set))
	}

	op := NewMapOperation()
	op.AddToSet("Set", []byte("Three"))

	_, mapoperr := Bucket("testsuitemap", "maps").Key(result.Key).MapOperation(op, res.RiakContext).Run(con())

	if mapoperr != nil {
		t.Error("MapOperr:", mapoperr)
	}

	var res2 testmapobject
	_, errget := bucket().Get(result.Key, &res2).Run(con())

	if errget != nil {
		t.Error("ErrGet:", errget)
	}

	if len(res2.Set) != 3 {
		t.Error("Unexpected length. Should be 3, got ", len(res2.Set))
		t.Errorf("%+v", res2)
	}
}
