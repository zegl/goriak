package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
	"reflect"
	"strconv"
)

func valueToOp(input interface{}) (*riak.MapOperation, error) {
	op := &riak.MapOperation{}

	var rValue reflect.Value
	var rType reflect.Type

	if reflect.ValueOf(input).Kind() == reflect.Struct {
		rValue = reflect.ValueOf(input)
		rType = reflect.TypeOf(input)
	} else if reflect.ValueOf(input).Kind() == reflect.Ptr {
		rValue = reflect.ValueOf(input).Elem()
		rType = reflect.TypeOf(input).Elem()
	} else {
		return nil, errors.New("Could not parse value. Needs to be struct or pointer to struct")
	}

	num := rType.NumField()

	for i := 0; i < num; i++ {
		field := rType.Field(i)

		itemKey := field.Name

		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			itemKey = tag
		}

		switch field.Type.Kind() {

		// Ints are saved as Registers
		case reflect.Int:
			op.SetRegister(itemKey, []byte(strconv.Itoa(int(rValue.Field(i).Int()))))

		// Strings are saved as Registers
		case reflect.String:
			op.SetRegister(itemKey, []byte(rValue.Field(i).String()))

		// Bools are saved as Flags
		case reflect.Bool:
			op.SetFlag(itemKey, rValue.Field(i).Bool())

		// Arrays are saved as Registers
		case reflect.Array:
			err := encodeArray(op, itemKey, rValue.Field(i))

			if err != nil {
				return nil, err
			}

		// Slices are saved as Sets
		// []byte and []uint8 are saved as Registers
		case reflect.Slice:
			err := encodeSlice(op, itemKey, rValue.Field(i))
			if err != nil {
				return nil, err
			}

		default:
			return nil, errors.New("Unexpected type: " + field.Type.Kind().String())
		}
	}

	return op, nil
}

// Arrays are saved as Registers
func encodeArray(op *riak.MapOperation, itemKey string, f reflect.Value) error {

	// Empty
	if f.Len() == 0 {
		op.SetRegister(itemKey, []byte{})
		return nil
	}

	// Byte array (uint8 is the same as byte)
	if f.Index(0).Kind() == reflect.Uint8 {
		register := make([]byte, f.Len())

		for ii := 0; ii < f.Len(); ii++ {
			register[ii] = uint8(f.Index(ii).Uint())
		}

		op.SetRegister(itemKey, register)

		return nil
	}

	return errors.New("Unkown Array type: " + f.Index(0).Kind().String())
}

// Slices are saved as Sets
// []byte and []uint8 are saved as Registers
func encodeSlice(op *riak.MapOperation, itemKey string, f reflect.Value) error {

	// Empty, do nothing
	if f.Len() == 0 {
		return nil
	}

	sliceType := f.Index(0).Kind()

	sliceLength := f.Len()
	sliceVal := f.Slice(0, sliceLength)

	switch sliceType {
	case reflect.Int:

		// Convert Int -> String -> []byte
		for ii := 0; ii < sliceLength; ii++ {
			intVal := sliceVal.Index(ii).Int()
			strVal := strconv.FormatInt(intVal, 10)
			op.AddToSet(itemKey, []byte(strVal))
		}

	case reflect.String:

		// Convert String -> []byte
		for ii := 0; ii < sliceLength; ii++ {
			strVal := sliceVal.Index(ii).String()
			op.AddToSet(itemKey, []byte(strVal))
		}

	case reflect.Uint8:

		// Uint8 is the same as byte, store the value directly
		op.SetRegister(itemKey, sliceVal.Bytes())

	default:
		return errors.New("Unknown slice type: " + sliceType.String())
	}

	return nil
}
