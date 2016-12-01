package goriak

import (
	"errors"
	"reflect"
	"strconv"
	"time"

	riak "github.com/basho/riak-go-client"
)

type mapEncoder struct {
	isModifyable bool
	riakRequest  requestData
}

func encodeInterface(input interface{}, riakRequest requestData) ([]byte, *riak.MapOperation, error) {
	op := &riak.MapOperation{}

	var rValue reflect.Value

	// Initialize encoder
	encoder := &mapEncoder{
		riakRequest: riakRequest,
	}

	if reflect.ValueOf(input).Kind() == reflect.Struct {
		rValue = reflect.ValueOf(input)
	} else if reflect.ValueOf(input).Kind() == reflect.Ptr {
		rValue = reflect.ValueOf(input).Elem()
		encoder.isModifyable = true
	} else {
		return []byte{}, nil, errors.New("Could not parse value. Needs to be struct or pointer to struct")
	}

	riakContext, err := encoder.encodeStruct(rValue, op, []string{})

	if err != nil {
		return []byte{}, nil, err
	}

	return riakContext, op, nil
}

func (e *mapEncoder) encodeStruct(rValue reflect.Value, op *riak.MapOperation, path []string) ([]byte, error) {
	rType := rValue.Type()

	num := rType.NumField()

	riakContext := []byte{}

	for i := 0; i < num; i++ {
		field := rType.Field(i)

		itemKey := field.Name

		tag := field.Tag.Get("goriak")

		if len(tag) > 0 {
			itemKey = tag

			// Use as context
			if tag == "goriakcontext" {
				riakContext = rValue.Field(i).Bytes()
			}

			// Ignore. Do not save this value.
			if tag == "-" {
				continue
			}
		}

		err := e.encodeValue(op, itemKey, rValue.Field(i), path)

		if err != nil {
			return []byte{}, err
		}
	}

	return riakContext, nil
}

func (e *mapEncoder) encodeValue(op *riak.MapOperation, itemKey string, f reflect.Value, path []string) error {
	switch f.Kind() {

	// Ints are saved as Registers
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		op.SetRegister(itemKey, []byte(strconv.FormatInt(f.Int(), 10)))

	// Strings are saved as Registers
	case reflect.String:
		op.SetRegister(itemKey, []byte(f.String()))

	// Bools are saved as Flags
	case reflect.Bool:
		op.SetFlag(itemKey, f.Bool())

	// Arrays are saved as Registers
	case reflect.Array:
		err := e.encodeArray(op, itemKey, f)

		if err != nil {
			return err
		}

	// Slices are saved as Sets
	// []byte and []uint8 are saved as Registers
	case reflect.Slice:
		err := e.encodeSlice(op, itemKey, f)

		if err != nil {
			return err
		}

	case reflect.Map:

		subPath := path
		subPath = append(subPath, itemKey)

		err := e.encodeMap(op, itemKey, f, subPath)

		if err != nil {
			return err
		}

	case reflect.Struct:

		done := false

		if ts, ok := f.Interface().(time.Time); ok {
			bin, err := ts.MarshalBinary()

			if err != nil {
				return err
			}

			op.SetRegister(itemKey, bin)
			done = true
		}

		_ = time.Time{}

		if !done {
			subOp := op.Map(itemKey)

			subPath := path
			subPath = append(subPath, itemKey)

			_, err := e.encodeStruct(f, subOp, subPath)

			if err != nil {
				return err
			}
		}

	case reflect.Ptr:
		ptrType := f.Type().String()

		// Counters
		if ptrType == "*goriak.Counter" {
			if f.IsNil() {
				// Increase by 0 to create the counter if it doesn't already exist
				op.IncrementCounter(itemKey, 0)

				// Initialize counter if Set() was given a struct pointer
				if e.isModifyable {
					resCounter := &Counter{
						helper: helper{
							name: itemKey,
							path: path,
							key:  e.riakRequest,
							//context: riakContext,
						},

						val: 0,
					}

					f.Set(reflect.ValueOf(resCounter))
				}

				return nil
			}

			counterValue := f.Elem().FieldByName("increaseBy").Int()
			op.IncrementCounter(itemKey, counterValue)

			return nil
		}

		// Set
		if ptrType == "*goriak.Set" {

			// Add an empty item
			if f.IsNil() {
				op.AddToSet(itemKey, []byte{})

				// Initialize counter if Set() was given a struct pointer
				if e.isModifyable {
					resSet := &Set{
						helper: helper{
							name: itemKey,
							path: path,
							key:  e.riakRequest,
							//context: riakContext,
						},
					}

					f.Set(reflect.ValueOf(resSet))
				}

				return nil
			}

			if s, ok := f.Interface().(*Set); ok {
				for _, add := range s.adds {
					op.AddToSet(itemKey, add)
				}

				for _, remove := range s.removes {
					op.RemoveFromSet(itemKey, remove)
				}
			}

			return nil
		}

		// Flag
		if ptrType == "*goriak.Flag" {

			// Add an empty flag
			if f.IsNil() {

				// Initialize flag if Flag() was given a struct pointer
				if e.isModifyable {
					resFlag := &Flag{
						helper: helper{
							name: itemKey,
							path: path,
							key:  e.riakRequest,
						},

						// Initialize to false
						val: false,
					}

					f.Set(reflect.ValueOf(resFlag))
				}

				return nil
			}

			// Save flag
			if f, ok := f.Interface().(*Flag); ok {
				op.SetFlag(itemKey, f.Value())
			}

			return nil
		}

		// Register
		if ptrType == "*goriak.Register" {

			// Add an empty flag
			if f.IsNil() {

				// Initialize flag if Flag() was given a struct pointer
				if e.isModifyable {
					resRegister := &Register{
						helper: helper{
							name: itemKey,
							path: path,
							key:  e.riakRequest,
						},
					}

					f.Set(reflect.ValueOf(resRegister))
				}

				return nil
			}

			// Save flag
			if f, ok := f.Interface().(*Register); ok {
				op.SetRegister(itemKey, f.Value())
			}

			return nil
		}

		return errors.New("Unexpected ptr type: " + f.Type().String())

	default:
		return errors.New("Unexpected type: " + f.Kind().String())
	}

	return nil
}

// Arrays are saved as Registers
func (e *mapEncoder) encodeArray(op *riak.MapOperation, itemKey string, f reflect.Value) error {

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

	return errors.New("Unknown Array type: " + f.Index(0).Kind().String())
}

// Slices are saved as Sets
// []byte and []uint8 are saved as Registers
func (e *mapEncoder) encodeSlice(op *riak.MapOperation, itemKey string, f reflect.Value) error {
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

func (e *mapEncoder) encodeMap(op *riak.MapOperation, itemKey string, f reflect.Value, path []string) error {
	keys := f.MapKeys()

	subOp := op.Map(itemKey)

	keyType := f.Type().Key().Kind()

	// Maps are not modifyable, save the current state and mark as not modifyable for now
	origModifyable := e.isModifyable

	e.isModifyable = false

	defer func() {
		e.isModifyable = origModifyable
	}()

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
			return errors.New("Unknown map key type: " + keyType.String())
		}

		err := e.encodeValue(subOp, keyString, f.MapIndex(key), path)

		if err != nil {
			return err
		}
	}

	return nil
}
