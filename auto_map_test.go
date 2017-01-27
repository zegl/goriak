package goriak

import (
	"reflect"
	"testing"
)

type testmapobject struct {
	A   string
	Set []string

	RiakContext []byte `goriak:"goriakcontext"`
}

func TestAutoMapSetAndGet(t *testing.T) {

	result, errset := bucket().Set(&testmapobject{
		A:   "Hello",
		Set: []string{"One", "Two"},
	}).Run(con())

	if errset != nil {
		t.Error("Set:", errset)
	}

	var res testmapobject

	result2, errget := bucket().Get(result.Key, &res).Run(con())

	if errget != nil {
		t.Error("Set:", errset)
	}

	if result2.NotFound {
		t.Error("Not found")
	}

	if res.A != "Hello" {
		t.Error("Unexpected A value")
	}

	if len(res.Set) != 2 {
		t.Error("Unexpected set length")
	}

	foundOne := false
	foundTwo := false

	for _, v := range res.Set {
		if v == "One" {
			foundOne = true
		}

		if v == "Two" {
			foundTwo = true
		}
	}

	if !foundOne || !foundTwo {
		t.Error("Unexpected set contents")
	}
}

func TestIsNotFound(t *testing.T) {
	var res testmapobject
	result, err := bucket().Get("unknown-key", &res).Run(con())

	if !result.NotFound {
		t.Error("not marked as not found")
	}

	if err == nil {
		t.Error("did not get error")
	}
}

func TestSetNonPointer(t *testing.T) {
	input := testmapobject{
		A: "I am passed as Value",
	}

	result, err := bucket().Set(input).Run(con())

	if err != nil {
		t.Error("Error: ", err.Error())
	}

	var res testmapobject
	result, err = bucket().Get(result.Key, &res).Run(con())

	if result.NotFound {
		t.Error("Not found")
	}

	if err != nil {
		t.Error(err)
	}

	if res.A != "I am passed as Value" {
		t.Error("Unknown response")
	}
}

func TestAbunchOfTypes(t *testing.T) {

	type customByteArray [3]byte

	type aBunchOfTypes struct {
		Int                  int
		String               string
		Array                [3]byte
		ByteSlice            []byte
		StringSlice          []string `goriak:"callme_string_slicer"`
		IntSlice             []int
		ByteSliceSlice       [][]byte
		ByteArraySlice       [][4]byte
		CustomByteArraySlice []customByteArray

		Num   int
		Num8  int8
		Num16 int16
		Num32 int32
		Num64 int64

		Unum   uint
		Unum8  uint8
		Unum16 uint16
		Unum32 uint32
		Unum64 uint64
	}

	o := aBunchOfTypes{
		Int:                  9001,
		String:               "Hello World",
		Array:                [3]byte{100, 101, 102},
		ByteSlice:            []byte{50, 60, 70},
		StringSlice:          []string{"H", "e", "l", "o"},
		IntSlice:             []int{4000, 5000, 6000},
		ByteSliceSlice:       [][]byte{{10, 11, 12}, {100, 110, 120}},
		ByteArraySlice:       [][4]byte{{1, 2, 3, 4}, {6, 6, 6, 6}},
		CustomByteArraySlice: []customByteArray{{1, 2, 3}, {4, 5, 6}},

		Num:   -1,
		Num8:  -8,
		Num16: -16,
		Num32: -32,
		Num64: -64,

		Unum:   1,
		Unum8:  8,
		Unum16: 16,
		Unum32: 32,
		Unum64: 64,
	}

	result, err := bucket().Set(o).Run(con())

	if err != nil {
		t.Error("Set", err)
	}

	var res aBunchOfTypes
	result, errGet := bucket().Get(result.Key, &res).Run(con())

	if errGet != nil {
		t.Error("Get", errGet)
	}

	if result.NotFound {
		t.Error("Not found")
	}

	if !reflect.DeepEqual(o, res) {
		t.Error("Not equal")
		t.Errorf("Got: %+v", res)
		t.Errorf("Expected: %+v", o)
	}

}

func TestFailNonMapType(t *testing.T) {
	_, err := bucket().Set(500).Run(con())

	if err == nil {
		t.Error("Did not receive error")
	}
}

func TestFailEmptyArray(t *testing.T) {
	type testType struct {
		A [0]byte
	}

	o := testType{}

	result, err := bucket().Set(o).Run(con())

	if err != nil {
		t.Error(err)
	}

	var res testType
	result, getErr := bucket().Get(result.Key, &res).Run(con())

	if result.NotFound {
		t.Error("not found")
	}

	if getErr != nil {
		t.Error(getErr)
	}
}

func TestUnsupportedArrayType(t *testing.T) {
	type testType struct {
		A [5]string
	}

	o := testType{}

	_, err := bucket().Set(o).Run(con())

	if err == nil {
		t.Error("Did not get error")
	}

	if err.Error() != "Unknown Array type: string" {
		t.Error("Unknown error")
		t.Error(err)
	}
}

func TestUnsupportedSliceType(t *testing.T) {
	type testType struct {
		A []bool
	}

	o := testType{
		A: []bool{false, true, true, true, false, true},
	}

	_, err := bucket().Set(o).Run(con())

	if err == nil {
		t.Error("Did not get error")
		return
	}

	if err.Error() != "Unknown slice type: bool" {
		t.Error("Unknown error")
		t.Error(err)
	}
}

func TestUnsupportedType(t *testing.T) {
	type testType struct {
		A [][]bool
	}

	o := testType{
		A: [][]bool{{true, false, true}},
	}

	_, err := bucket().Set(o).Run(con())

	if err == nil {
		t.Error("Did not get error")
		return
	}

	if err.Error() != "Unknown slice slice type: bool" {
		t.Error("Unknown error")
		t.Error(err)
	}
}

func TestMapBool(t *testing.T) {
	type testType struct {
		A bool
		B bool
	}

	o := testType{
		A: true,
		B: false,
	}

	result, err := bucket().Set(o).Run(con())

	if err != nil {
		t.Error(err)
		return
	}

	var res testType
	result, err = bucket().Get(result.Key, &res).Run(con())

	if err != nil {
		t.Error(err)
	}

	if result.NotFound {
		t.Error("Not Found")
	}

	if !res.A {
		t.Error("A was not true")
	}

	if res.B {
		t.Error("B was not false")
	}
}

func TestUnknownTypeFloat(t *testing.T) {
	type ourTestType struct {
		foo float64
	}

	item := ourTestType{
		foo: 12.34,
	}

	_, err := bucket().Set(item).Run(con())

	if err == nil {
		t.Error("Did not get error")
	}

	if err != nil && err.Error() != "Unexpected type: float64" {
		t.Error("Unknown error")
		t.Error(err)
	}
}

func TestEmptyStruct(t *testing.T) {
	type aBunchOfTypes struct {
		Int            int
		String         string
		Array          [3]byte
		ByteSlice      []byte
		StringSlice    []string `goriak:"callme_string_slicer"`
		IntSlice       []int
		ByteSliceSlice [][]byte
	}

	item := aBunchOfTypes{}

	_, err := bucket().Set(item).Run(con())

	if err != nil {
		t.Error(err)
	}
}

func TestMapInStruct(t *testing.T) {
	type ourTestType struct {
		Foo       string
		Bar       map[int]string
		Bar8      map[int8]string
		Bar16     map[int16]string
		Bar32     map[int32]string
		Bar64     map[int64]string
		BarByte   map[int64][]byte
		BarString map[string]string
	}

	item := ourTestType{
		Foo: "Foo",
		Bar: map[int]string{
			10: "Ten",
			20: "Twenty",
		},
		Bar8: map[int8]string{
			10: "Ten",
			20: "Twenty",
		},
		Bar16: map[int16]string{
			10: "Ten",
			20: "Twenty",
		},
		Bar32: map[int32]string{
			10: "Ten",
			20: "Twenty",
		},
		Bar64: map[int64]string{
			10: "Ten",
			20: "Twenty",
		},

		BarByte: map[int64][]byte{
			4000:  {1, 2, 3, 4, 5},
			10000: {50, 60, 70, 80},
		},

		BarString: map[string]string{
			"Ten":    "TenTen",
			"Twenty": "TewntyTwenty",
		},
	}

	result, err := bucket().Set(item).Run(con())

	if err != nil {
		t.Error("Set", err)
	}

	var res ourTestType
	_, err = bucket().Get(result.Key, &res).Run(con())

	if err != nil {
		t.Error("Get", err)
	}

	if !reflect.DeepEqual(item, res) {
		t.Error("Not equal")
		t.Errorf("Got: %+v", res)
		t.Errorf("Expected: %+v", item)
	}
}

func TestSubStructs(t *testing.T) {
	type ourSubTestType struct {
		AA string
		BB string
	}

	type ourOtherSubTestType struct {
		DD ourSubTestType
	}

	type ourTestType struct {
		A string
		B ourSubTestType
		C ourOtherSubTestType
	}

	item := ourTestType{
		A: "Outer A",
		B: ourSubTestType{
			AA: "Inner A",
			BB: "Inner B",
		},
		C: ourOtherSubTestType{
			DD: ourSubTestType{
				AA: "Other A",
				BB: "Other B",
			},
		},
	}

	result, err := bucket().Set(item).Run(con())

	if err != nil {
		t.Error(err)
	}

	var res ourTestType
	_, err = bucket().Get(result.Key, &res).Run(con())

	if err != nil {
		t.Error("Get", err)
	}

	if !reflect.DeepEqual(item, res) {
		t.Error("Not equal")
		t.Errorf("Got: %+v", res)
		t.Errorf("Expected: %+v", item)
	}
}

func TestAutoMapSlices(t *testing.T) {
	type writeType struct {
		A string
	}

	result, err := bucket().Set(writeType{
		A: "aaaa",
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	type readType struct {
		A string
		B float64
	}

	var res readType
	_, err = bucket().Get(result.Key, &res).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown type: float64" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type readType2 struct {
		A string
		B [][]bool
	}

	var res2 readType2
	_, err = bucket().Get(result.Key, &res2).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown slice slice type: bool" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type readType3 struct {
		A string
		B [][5]bool
	}

	var res3 readType3
	_, err = bucket().Get(result.Key, &res3).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown slice array type: bool" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type readType4 struct {
		A string
		B []float64
	}

	var res4 readType4
	_, err = bucket().Get(result.Key, &res4).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown slice type: float64" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type writeType5 struct {
		A string
		B map[string]string
	}

	result, err = bucket().Set(writeType5{
		A: "aaaa",
		B: map[string]string{
			"AA": "BB",
			"CC": "DD",
		},
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	type readType5 struct {
		A string
		B map[float64]float64
	}

	var res5 readType5
	_, err = bucket().Get(result.Key, &res5).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown map key type: float64" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type writeType6sub struct {
		AA string
	}

	type writeType6 struct {
		A string
		B writeType6sub
	}

	result, err = bucket().Set(writeType6{
		A: "aaaa",
		B: writeType6sub{
			AA: "bbbb",
		},
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	type readType6sub struct {
		AA float64
	}

	type readType6 struct {
		A string
		B readType6sub
	}

	var res6 readType6
	_, err = bucket().Get(result.Key, &res6).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown type: float64" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type writeType7 struct {
		A string
		B map[string]string
	}

	result, err = bucket().Set(writeType5{
		A: "aaaa",
		B: map[string]string{
			"AA": "BB",
			"CC": "DD",
		},
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	type readType7 struct {
		A string
		B map[string]float64
	}

	var res7 readType7
	_, err = bucket().Get(result.Key, &res7).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown map value type" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type readType7b struct {
		A string
		B map[string][]float64
	}

	var res7b readType7b
	_, err = bucket().Get(result.Key, &res7b).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != "Unknown map value type" {
		t.Error("Unexpected error", err)
	}

	// ---------

	type writeType8 struct {
		A string
		B []string
	}

	result, err = bucket().Set(writeType8{
		A: "aaaa",
		B: []string{"a", "b"},
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	type readType8 struct {
		A string
		B []int
	}

	var res8 readType8
	_, err = bucket().Get(result.Key, &res8).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != `strconv.ParseInt: parsing "a": invalid syntax` {
		t.Error("Unexpected error", err)
	}

	// ---------

	type writeType9 struct {
		A *string
	}

	s := "ptr string"

	result, err = bucket().Set(writeType9{
		A: &s,
	}).Run(con())

	if err == nil || err.Error() != "Unexpected ptr type: *string" {
		t.Error(err)
	}

	result, err = bucket().Set(writeType{
		A: "not a pointer",
	}).Run(con())

	if err != nil {
		t.Error(err)
	}

	var res9 writeType9
	_, err = bucket().Get(result.Key, &res9).Run(con())

	if err == nil {
		t.Error("No error")
		return
	}

	if err.Error() != `Unexpected ptr type: *string` {
		t.Error("Unexpected error", err)
	}
}

func TestEncodeErrors(t *testing.T) {
	type writeType1 struct {
		A map[float64]string
	}

	_, err := bucket().Set(writeType1{
		A: map[float64]string{
			2.0: "2",
			3.0: "3",
		},
	}).Run(con())

	if err == nil {
		t.Error("no error")
	}

	if err.Error() != "Unknown map key type: float64" {
		t.Error("Unexpected error", err)
	}

	// ----------

	type writeType2 struct {
		A map[int]float64
	}

	_, err = bucket().Set(writeType2{
		A: map[int]float64{
			2: 2.0,
			3: 3.0,
		},
	}).Run(con())

	if err == nil {
		t.Error("no error")
	}

	if err.Error() != "Unexpected type: float64" {
		t.Error("Unexpected error", err)
	}

}

func TestAutoMapIgnore(t *testing.T) {
	type ourTestType struct {
		SaveA   string `goriak:"save_a"`
		SaveB   string `goriak:"save_b"`
		IgnoreC string `goriak:"-"`
	}

	val := ourTestType{
		SaveA:   "SaveA",
		SaveB:   "SaveB",
		IgnoreC: "IgnoreC",
	}

	c := con()

	res, err := bucket().Set(val).Run(c)

	if err != nil {
		t.Error(err)
	}

	var output ourTestType
	_, err = bucket().Get(res.Key, &output).Run(c)

	if err != nil {
		t.Error(err)
	}

	if output.IgnoreC != "" {
		t.Error("IgnoreC had a value:", output.IgnoreC)
	}

	if output.SaveA != "SaveA" || output.SaveB != "SaveB" {
		t.Error("Unepxected output content")
		t.Logf("%+v", output)
	}
}

func TestAutoMapMapArray(t *testing.T) {
	type ourTestType struct {
		Things map[string][4]byte
	}

	val := ourTestType{
		Things: map[string][4]byte{
			"a": {1, 1, 1, 1},
			"b": {2, 2, 2, 2},
		},
	}

	c := con()

	res, err := bucket().Set(val).Run(c)

	if err != nil {
		t.Error(err)
	}

	var resVal ourTestType
	_, err = bucket().Get(res.Key, &resVal).Run(c)

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(val, resVal) {
		t.Error("Did not get same value back")
		t.Log(val)
		t.Log(resVal)
	}
}

func TestAutoMapMapArray2(t *testing.T) {
	type ourTestType struct {
		Things map[int64][32]byte
	}

	val := ourTestType{
		Things: map[int64][32]byte{
			500: {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6},
			600: {2, 2, 2, 2},
		},
	}

	c := con()

	res, err := bucket().Set(val).Run(c)

	if err != nil {
		t.Error(err)
	}

	var resVal ourTestType
	_, err = bucket().Get(res.Key, &resVal).Run(c)

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(val, resVal) {
		t.Error("Did not get same value back")
		t.Log(val)
		t.Log(resVal)
	}
}

func TestAutoMapGetNoBucket(t *testing.T) {
	var val string
	_, err := Bucket("", "").Get("keykeykey", val).Run(con())

	if err == nil {
		t.Error("no error")
		return
	}

	if err.Error() != "ClientError|Bucket is required" {
		t.Error("Unexpected error:", err.Error())
	}
}

func TestInsertAndGet(t *testing.T) {
	c, _ := Connect(ConnectOpts{
		Address: "127.0.0.1",
	})

	type ourTestType struct {
		A string
		B []string
	}

	val := ourTestType{
		A: "AAA",
		B: []string{"B", "BB", "BBB"},
	}

	res, err := Bucket("testsuitemap", "maps").Set(val).Run(c)

	if err != nil {
		t.Error("Insert:", err)
	}

	if len(res.Key) < 20 {
		t.Error("Unexpected key")
	}

	var out ourTestType
	res, err = Bucket("testsuitemap", "maps").Get(res.Key, &out).Run(c)

	if err != nil {
		t.Error("Get:", err)
	}

	if !reflect.DeepEqual(val, out) {
		t.Error("Not equal")
	}
}
