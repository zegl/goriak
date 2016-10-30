package goriak

import (
	"errors"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

func encodeInterface(input interface{}) (*riak.MapOperation, error) {
	op := &riak.MapOperation{}

	var rValue reflect.Value

	if reflect.ValueOf(input).Kind() == reflect.Struct {
		rValue = reflect.ValueOf(input)
	} else if reflect.ValueOf(input).Kind() == reflect.Ptr {
		rValue = reflect.ValueOf(input).Elem()
	} else {
		return nil, errors.New("Could not parse value. Needs to be struct or pointer to struct")
	}

	err := encodeStruct(rValue, op)

	if err != nil {
		return nil, err
	}

	return op, nil
}

func encodeStruct(rValue reflect.Value, op *riak.MapOperation) error {
	rType := rValue.Type()

	num := rType.NumField()

	for i := 0; i < num; i++ {
		field := rType.Field(i)

		itemKey := field.Name

		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			itemKey = tag
		}

		err := encodeValue(op, itemKey, rValue.Field(i))

		if err != nil {
			return err
		}
	}

	return nil
}

func encodeValue(op *riak.MapOperation, itemKey string, f reflect.Value) error {
	switch f.Kind() {

	// Ints are saved as Registers
	case reflect.Int:
		op.SetRegister(itemKey, []byte(strconv.Itoa(int(f.Int()))))

	// Strings are saved as Registers
	case reflect.String:
		op.SetRegister(itemKey, []byte(f.String()))

	// Bools are saved as Flags
	case reflect.Bool:
		op.SetFlag(itemKey, f.Bool())

	// Arrays are saved as Registers
	case reflect.Array:
		err := encodeArray(op, itemKey, f)

		if err != nil {
			return err
		}

	// Slices are saved as Sets
	// []byte and []uint8 are saved as Registers
	case reflect.Slice:
		err := encodeSlice(op, itemKey, f)
		if err != nil {
			return err
		}

	case reflect.Map:
		err := encodeMap(op, itemKey, f)

		if err != nil {
			return err
		}

	case reflect.Struct:
		subOp := op.Map(itemKey)
		encodeStruct(f, subOp)

	case reflect.Ptr:

		switch f.Type().String() {

		case "*goriak.Counter": // Counters
			if f.IsNil() {
				// Increase by 0 to create the counter if it doesn't already exist
				op.IncrementCounter(itemKey, 0)
				return nil
			}

			counterValue := f.Elem().FieldByName("increaseBy").Int()
			op.IncrementCounter(itemKey, counterValue)

		case "*goriak.Set": // Set

			// Add an empty item
			if f.IsNil() {
				op.AddToSet(itemKey, []byte{})
				return nil
			}

			iface := f.Elem().Interface()

			if s, ok := iface.(*Set); ok {
				for _, add := range s.adds {
					op.AddToSet(itemKey, add)
				}

				for _, remove := range s.removes {
					op.RemoveFromSet(itemKey, remove)
				}
			} else {
				return errors.New("Could not convert to *Set?")
			}

		default:
			return errors.New("Unexpected ptr type: " + f.Type().String())
		}

	default:
		return errors.New("Unexpected type: " + f.Kind().String())
	}

	return nil
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
	sliceType := f.Type().Elem().Kind()
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

	case reflect.Array:

		// [n]byte
		if sliceVal.Type().Elem().Elem().Kind() == reflect.Uint8 {
			for ii := 0; ii < sliceVal.Len(); ii++ {

				item := sliceVal.Index(ii)

				// Convert the array to a byte slice
				byteValue := make([]byte, item.Len())

				for byteValueIndex := 0; byteValueIndex < item.Len(); byteValueIndex++ {
					byteValue[byteValueIndex] = uint8(item.Index(byteValueIndex).Uint())
				}

				op.AddToSet(itemKey, byteValue)
			}

			return nil
		}

	case reflect.Slice:

		// Empty, do nothing
		if sliceVal.Len() == 0 {
			return nil
		}

		if sliceVal.Type().Elem().Elem().Kind() == reflect.Uint8 {
			for ii := 0; ii < sliceVal.Len(); ii++ {
				op.AddToSet(itemKey, sliceVal.Index(ii).Bytes())
			}

			return nil
		}

		return errors.New("Unknown slice slice type: " + sliceVal.Type().Elem().Elem().Kind().String())

	default:
		return errors.New("Unknown slice type: " + sliceType.String())
	}

	return nil
}

func encodeMap(op *riak.MapOperation, itemKey string, f reflect.Value) error {
	keys := f.MapKeys()

	subOp := op.Map(itemKey)

	keyType := f.Type().Key().Kind()

	for _, key := range keys {

		// Convert the key to string
		var keyString string
		switch keyType {
		case reflect.String:
			keyString = key.String()

		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			keyString = strconv.FormatInt(int64(key.Int()), 10)
		default:
			return errors.New("Unknown map key type")
		}

		err := encodeValue(subOp, keyString, f.MapIndex(key))

		if err != nil {
			return err
		}
	}

	return nil
}
