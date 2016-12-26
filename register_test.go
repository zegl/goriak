package goriak

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegister(t *testing.T) {
	f := NewRegister()
	assert.Empty(t, f.Value())

	f.SetString("hello")
	assert.Equal(t, f.String(), "hello")
}

func TestRegisterMap(t *testing.T) {
	type ourTestType struct {
		Name *Register
	}

	c := con()

	key := randomKey()

	val := ourTestType{}
	_, err := bucket().Key(key).Set(&val).Run(c)
	assert.Nil(t, err)
	assert.Empty(t, val.Name.Value())
	assert.Empty(t, val.Name.String())

	// Update Register
	err = val.Name.Set([]byte("foo")).Exec(c)
	assert.Nil(t, err)
	assert.Equal(t, val.Name.String(), "foo")

	// Get
	var val2 ourTestType
	_, err = bucket().Get(key, &val2).Run(c)
	assert.Nil(t, err)
	assert.Equal(t, val2.Name.String(), "foo")

	// Update via Set
	val.Name.SetString("bar")
	_, err = bucket().Key(key).Set(&val).Run(c)
	assert.Nil(t, err)

	// Get
	var val3 ourTestType
	_, err = bucket().Get(key, &val3).Run(c)
	assert.Nil(t, err)
	assert.Equal(t, val3.Name.String(), "bar")
}

func TestNestedHelperTypes(t *testing.T) {
	type ourTestType struct {
		A struct {
			B struct {
				Register *Register
				Flag     *Flag
				Set      *Set
				Counter  *Counter
			}
		}
	}

	var val ourTestType
	key := randomKey()
	c := con()

	_, err := bucket().Key(key).Set(&val).Run(c)
	assert.Nil(t, err)

	// Register
	err = val.A.B.Register.SetString("foobar").Exec(c)
	assert.Nil(t, err)
	assert.Equal(t, val.A.B.Register.String(), "foobar")

	// Flag
	err = val.A.B.Flag.Set(true).Exec(c)
	assert.Nil(t, err)
	assert.Equal(t, val.A.B.Flag.Value(), true)

	// Set
	err = val.A.B.Set.AddString("foo").AddString("bar").Exec(c)
	assert.Nil(t, err)
	assert.Equal(t, val.A.B.Set.HasString("foo"), true)
	assert.Equal(t, val.A.B.Set.HasString("bar"), true)
	assert.Equal(t, val.A.B.Set.HasString("baz"), false)

	// Counter
	err = val.A.B.Counter.Increase(3).Exec(c)
	assert.Nil(t, err)
	assert.Equal(t, val.A.B.Counter.Value(), int64(3))

	// Get it again
	var val2 ourTestType
	_, err = bucket().Get(key, &val2).Run(c)
	assert.Nil(t, err)

	assert.Equal(t, val.A.B.Register.Value(), val2.A.B.Register.Value())
	assert.Equal(t, val.A.B.Flag.Value(), val2.A.B.Flag.Value())
	assert.Equal(t, val.A.B.Set.Value(), val2.A.B.Set.Value())
	assert.Equal(t, val.A.B.Counter.Value(), val2.A.B.Counter.Value())
}

func TestHelperTypeOnNil(t *testing.T) {
	type ourTestType struct {
		Register *Register
		Flag     *Flag
		Set      *Set
		Counter  *Counter
	}

	c := con()

	var val ourTestType

	err := val.Register.Exec(c)
	assert.EqualError(t, err, "Nil Register")

	err = val.Flag.Exec(c)
	assert.EqualError(t, err, "Nil Flag")

	err = val.Set.Exec(c)
	assert.EqualError(t, err, "Nil Set")

	err = val.Counter.Exec(c)
	assert.EqualError(t, err, "Nil Counter")
}

func TestHelperTypeUnknownPath(t *testing.T) {
	type ourTestType struct {
		Register *Register
		Flag     *Flag
		Set      *Set
		Counter  *Counter
	}

	c := con()

	val := ourTestType{
		Register: NewRegister(),
		Flag:     NewFlag(),
		Set:      NewSet(),
		Counter:  NewCounter(),
	}

	err := val.Register.Exec(c)
	assert.EqualError(t, err, "Unknown path to Register. Retrieve Register with Get or Set before updating the Register")

	err = val.Flag.Exec(c)
	assert.EqualError(t, err, "Unknown path to Flag. Retrieve Flag with Get or Set before updating the Flag")

	err = val.Set.Exec(c)
	assert.EqualError(t, err, "Unknown path to Set. Retrieve Set with Get or Set before updating the Set")

	err = val.Counter.Exec(c)
	assert.EqualError(t, err, "Unknown path to Counter. Retrieve Counter with Get or Set before updating the Counter")
}

func TestRegisterJSON(t *testing.T) {
	c := NewRegister()
	c.SetString("hello")

	jstr, err := json.Marshal(c)

	if err != nil {
		t.Error(err)
	}

	var newRegister *Register

	err = json.Unmarshal(jstr, &newRegister)

	if err != nil {
		t.Error(err)
	}

	if c.String() != "hello" {
		t.Error("Unexpected value")
	}
}
