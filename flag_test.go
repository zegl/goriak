package goriak

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFlag(t *testing.T) {
	f := NewFlag()
	assert.False(t, f.Value())

	f.Set(true)
	assert.True(t, f.Value())
}

func TestFlagMap(t *testing.T) {

	type ourTestType struct {
		Enabled *Flag
	}

	c := con()

	key := randomKey()

	val := ourTestType{}
	_, err := bucket().Key(key).Set(&val).Run(c)

	assert.Nil(t, err)
	assert.False(t, val.Enabled.Value())

	// Update Flag
	err = val.Enabled.Set(true).Exec(c)
	assert.Nil(t, err)
	assert.True(t, val.Enabled.Value())

	// Set flag to false
	err = val.Enabled.Set(false).Exec(c)
	assert.Nil(t, err)
	assert.False(t, val.Enabled.Value())

	// Update via Set
	val.Enabled.Set(true)
	_, err = bucket().Key(key).Set(&val).Run(c)
	assert.Nil(t, err)

	// Get
	var val2 ourTestType
	_, err = bucket().Get(key, &val2).Run(c)
	assert.Nil(t, err)
	assert.True(t, val2.Enabled.Value())
}

func TestFlagJSON(t *testing.T) {
	c := NewFlag()
	c.Set(true)

	jstr, err := json.Marshal(c)

	if err != nil {
		t.Error(err)
	}

	var newFlag *Flag

	err = json.Unmarshal(jstr, &newFlag)

	if err != nil {
		t.Error(err)
	}

	if c.Value() == false {
		t.Error("Unexpected value")
	}
}
