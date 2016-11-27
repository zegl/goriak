package goriak

import (
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
	err = val.Name.SetString("foo").Exec(c)
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
