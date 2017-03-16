package goriak

import (
	"errors"
	"reflect"
	"strconv"
	"time"

	riak "gopkg.in/zegl/goriak.v3/deps/riak-go-client"
)

func decodeInterface(data *riak.FetchMapResponse, output interface{}, riakRequest requestData) error {
	return transMapToStruct(
		data.Map,
		reflect.ValueOf(output).Elem(),
		reflect.TypeOf(output).Elem(),
		data.Context,
		[]string{}, // Start with an empty path
		riakRequest,
	)
}

// Assings values from a Riak Map to a receiving Go struct
func transMapToStruct(data *riak.Map, rValue reflect.Value, rType reflect.Type, riakContext []byte, path []string, riakRequest requestData) error {

	num := rType.NumField()

	for i := 0; i < num; i++ {

		field := rType.Field(i)
		fieldVal := rValue.Field(i)
		registerName := field.Name
		tag := field.Tag.Get("goriak")

		// goriakcontext is a reserved keyword.
		// Use the tag `goriak:"goriakcontext"` to get the Riak context necessary for certaion Riak operations,
		// such as removing items from a Set.
		if tag == "goriakcontext" {
			rValue.Field(i).SetBytes(riakContext)
			continue
		}

		// Ignore this value
		if tag == "-" {
			continue
		}

		if len(tag) > 0 {
			registerName = tag
		}

		switch field.Type.Kind() {
		case reflect.Array:
			fallthrough
		case reflect.String:
			fallthrough
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			fallthrough
		case reflect.Uint:
			fallthrough
		case reflect.Uint8:
			fallthrough
		case reflect.Uint16:
			fallthrough
		case reflect.Uint32:
			fallthrough
		case reflect.Uint64:
			if val, ok := data.Registers[registerName]; ok {
				if newVal, err := bytesToValue(val, field.Type); err == nil {
					fieldVal.Set(newVal)
				}
			}

		case reflect.Bool:
			if val, ok := data.Flags[registerName]; ok {
				fieldVal.SetBool(val)
			}

		case reflect.Slice:
			err := transRiakToSlice(rValue.Field(i), registerName, data)

			if err != nil {
				return err
			}

		case reflect.Map:
			if subMap, ok := data.Maps[registerName]; ok {
				err := transMapToMap(rValue.Field(i), subMap)

				if err != nil {
					return err
				}
			}

		case reflect.Struct:
			done := false

			// time.Time
			if bin, ok := data.Registers[registerName]; ok {
				if ts, ok := fieldVal.Interface().(time.Time); ok {
					err := ts.UnmarshalBinary(bin)

					if err != nil {
						return err
					}

					fieldVal.Set(reflect.ValueOf(ts))
					done = true
				}
			}

			if !done {

				if subMap, ok := data.Maps[registerName]; ok {
					// Struct
					newPath := append(path, registerName)

					err := transMapToStruct(subMap, fieldVal, fieldVal.Type(), riakContext, newPath, riakRequest)

					if err != nil {
						return err
					}
				}
			}

		case reflect.Ptr:

			helperPathData := helper{
				name:    registerName,
				path:    path,
				key:     riakRequest,
				context: riakContext,
			}

			switch fieldVal.Type().String() {
			case "*goriak.Counter":
				var counterValue int64

				if val, ok := data.Counters[registerName]; ok {
					counterValue = val
				}

				resCounter := &Counter{
					helper: helperPathData,
					val:    counterValue,
				}

				fieldVal.Set(reflect.ValueOf(resCounter))

			case "*goriak.Set":

				var setValue [][]byte

				if val, ok := data.Sets[registerName]; ok {
					setValue = val
				}

				resSet := &Set{
					helper: helperPathData,
					value:  setValue,
				}

				// Cleans the object for empty objects
				// This is because of backwards compatibility reasons with goriak <= 2.4
				resSet.removeEmptyItems()

				fieldVal.Set(reflect.ValueOf(resSet))

			case "*goriak.Flag":

				var flagValue bool

				if val, ok := data.Flags[registerName]; ok {
					flagValue = val
				}

				resFlag := &Flag{
					helper: helperPathData,
					val:    flagValue,
				}

				fieldVal.Set(reflect.ValueOf(resFlag))

			case "*goriak.Register":

				var registerValue []byte

				if val, ok := data.Registers[registerName]; ok {
					registerValue = val
				}

				resRegister := &Register{
					helper: helperPathData,
					val:    registerValue,
				}

				fieldVal.Set(reflect.ValueOf(resRegister))

			default:
				return errors.New("Unexpected ptr type: " + fieldVal.Type().String())
			}

		default:
			return errors.New("Unknown type: " + field.Type.Kind().String())
		}
	}

	return nil
}

// Converts Riak objects (can be either Sets or Registers) to Golang Slices
func transRiakToSlice(sliceValue reflect.Value, registerName string, data *riak.Map) error {

	switch sliceValue.Type().Elem().Kind() {

	// []int
	case reflect.Int:
		if setVal, ok := data.Sets[registerName]; ok {
			result := make([]int, len(setVal))

			for i, v := range setVal {
				intVal, err := strconv.ParseInt(string(v), 10, 64)

				if err != nil {
					return err
				}

				result[i] = int(intVal)
			}

			// Success!
			sliceValue.Set(reflect.ValueOf(result))
		}

	// []string
	case reflect.String:
		if setVal, ok := data.Sets[registerName]; ok {
			result := make([]string, len(setVal))

			for i, v := range setVal {
				result[i] = string(v)
			}

			// Success!
			sliceValue.Set(reflect.ValueOf(result))
		}

	// []byte
	case reflect.Uint8:
		if val, ok := data.Registers[registerName]; ok {
			sliceValue.SetBytes(val)
		}

	// [][]byte
	case reflect.Slice:

		if sliceValue.Type().Elem().Elem().Kind() == reflect.Uint8 {
			if val, ok := data.Sets[registerName]; ok {
				sliceValue.Set(reflect.ValueOf(val))
			}

			return nil
		}

		return errors.New("Unknown slice slice type: " + sliceValue.Type().Elem().Elem().Kind().String())

	// [][n]byte
	case reflect.Array:
		if sliceValue.Type().Elem().Elem().Kind() == reflect.Uint8 {
			if values, ok := data.Sets[registerName]; ok {

				lengthOfExpectedArray := sliceValue.Type().Elem().Len()

				// The type of the inner array
				arrayType := sliceValue.Type().Elem()

				// A slice with array Type items
				// The length is set to the amount of values in the Set from Riak
				sliceType := reflect.SliceOf(arrayType)
				finalSliceValue := reflect.MakeSlice(sliceType, len(values), len(values))

				for valueIndex, value := range values {

					// Create the array from Riak data
					newArray := reflect.New(arrayType).Elem()

					for i := 0; i < lengthOfExpectedArray; i++ {
						newArray.Index(i).Set(reflect.ValueOf(value[i]))
					}

					// Add array to slice
					finalSliceValue.Index(valueIndex).Set(newArray)
				}

				// Override the Slice from "Userland"
				sliceValue.Set(finalSliceValue)
			}

			return nil
		}

		return errors.New("Unknown slice array type: " + sliceValue.Type().Elem().Elem().Kind().String())

	default:
		return errors.New("Unknown slice type: " + sliceValue.Type().Elem().Kind().String())
	}

	return nil
}

func bytesToValue(input []byte, outputType reflect.Type) (reflect.Value, error) {

	outputKind := outputType.Kind()

	// The final type (can be a custom type for example)
	newWithSameType := reflect.New(outputType).Elem()

	switch outputKind {
	case reflect.String:
		newWithSameType.SetString(string(input))
		return newWithSameType, nil

	case reflect.Int:
		if i, err := strconv.ParseInt(string(input), 10, 0); err == nil {
			newWithSameType.SetInt(i)
			return newWithSameType, nil
		}

	case reflect.Int8:
		if i, err := strconv.ParseInt(string(input), 10, 8); err == nil {
			newWithSameType.SetInt(i)
			return newWithSameType, nil
		}

	case reflect.Int16:
		if i, err := strconv.ParseInt(string(input), 10, 16); err == nil {
			newWithSameType.SetInt(i)
			return newWithSameType, nil
		}

	case reflect.Int32:
		if i, err := strconv.ParseInt(string(input), 10, 32); err == nil {
			newWithSameType.SetInt(i)
			return newWithSameType, nil
		}

	case reflect.Int64:
		if i, err := strconv.ParseInt(string(input), 10, 64); err == nil {
			newWithSameType.SetInt(i)
			return newWithSameType, nil
		}

	case reflect.Uint:
		if i, err := strconv.ParseUint(string(input), 10, 0); err == nil {
			newWithSameType.SetUint(i)
			return newWithSameType, nil
		}

	case reflect.Uint8:
		if i, err := strconv.ParseUint(string(input), 10, 8); err == nil {
			newWithSameType.SetUint(i)
			return newWithSameType, nil
		}

	case reflect.Uint16:
		if i, err := strconv.ParseUint(string(input), 10, 16); err == nil {
			newWithSameType.SetUint(i)
			return newWithSameType, nil
		}

	case reflect.Uint32:
		if i, err := strconv.ParseUint(string(input), 10, 32); err == nil {
			newWithSameType.SetUint(i)
			return newWithSameType, nil
		}

	case reflect.Uint64:
		if i, err := strconv.ParseUint(string(input), 10, 64); err == nil {
			newWithSameType.SetUint(i)
			return newWithSameType, nil
		}

	case reflect.Slice:
		sliceItemType := outputType.Elem().Kind()

		switch sliceItemType {
		case reflect.Uint8:
			return reflect.ValueOf(input), nil
		}

	case reflect.Array:

		// Create new array of the expected type
		newArray := reflect.New(outputType).Elem()
		lengthOfExpectedArray := outputType.Len()
		arrayItemType := outputType.Elem().Kind()

		switch arrayItemType {
		// Byte array
		case reflect.Uint8:

			// Copy bytes
			for i := 0; i < lengthOfExpectedArray; i++ {
				newArray.Index(i).Set(reflect.ValueOf(input[i]))
			}

			return newArray, nil
		}
	}

	return reflect.ValueOf(nil), errors.New("Invalid input type: " + outputType.String())
}

// Converts a Riak Map to a Go Map
func transMapToMap(mapValue reflect.Value, data *riak.Map) error {

	mapKeyType := mapValue.Type().Key().Kind()

	// Initialize the map
	newMap := reflect.MakeMap(mapValue.Type())
	mapValue.Set(newMap)

	for key, val := range data.Registers {

		// Convert key (a string) to the correct reflect.Value
		keyValue, err := bytesToValue([]byte(key), mapValue.Type().Key())

		if err != nil {
			return errors.New("Unknown map key type: " + mapKeyType.String())
		}

		valValue, err := bytesToValue(val, mapValue.Type().Elem())

		if err != nil {
			return errors.New("Unknown map value type")
		}

		// Save value to the Go map
		mapValue.SetMapIndex(keyValue, valValue)
	}

	return nil
}
