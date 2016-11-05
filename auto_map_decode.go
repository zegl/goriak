package goriak

import (
	"errors"
	"reflect"
	"strconv"

	riak "github.com/basho/riak-go-client"
)

func decodeInterface(data *riak.FetchMapResponse, output interface{}, riakRequest requestData) error {
	return mapToStruct(
		data.Map,
		reflect.ValueOf(output).Elem(),
		reflect.TypeOf(output).Elem(),
		data.Context,
		[]string{}, // Start with an empty path
		riakRequest,
	)
}

func mapToStruct(data *riak.Map, rValue reflect.Value, rType reflect.Type, riakContext []byte, path []string, riakRequest requestData) error {
	num := rType.NumField()

	for i := 0; i < num; i++ {

		field := rType.Field(i)
		registerName := field.Name
		tag := field.Tag.Get("goriak")

		// goriakcontext is a reserved keyword.
		// Use the tag `goriak:"goriakcontext"` to get the Riak context necessary for certaion Riak operations,
		// such as removing items from a Set.
		if tag == "goriakcontext" {
			rValue.Field(i).SetBytes(riakContext)
			continue
		}

		if len(tag) > 0 {
			registerName = tag
		}

		switch field.Type.Kind() {
		case reflect.String:
			if val, ok := data.Registers[registerName]; ok {
				rValue.Field(i).SetString(string(val))
			}

		case reflect.Array:
			// []byte
			if rValue.Field(i).Type().Elem().Kind() == reflect.Uint8 {
				if val, ok := data.Registers[registerName]; ok {
					for ii := 0; ii < rValue.Field(i).Len(); ii++ {
						rValue.Field(i).Index(ii).SetUint(uint64(val[ii]))
					}
				}
			}

		// Integer types
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:

			if val, ok := data.Registers[registerName]; ok {
				intVal, err := strconv.ParseInt(string(val), 10, 0)

				if err == nil {
					rValue.Field(i).SetInt(intVal)
				}
			}

		case reflect.Bool:

			if val, ok := data.Flags[registerName]; ok {
				rValue.Field(i).SetBool(val)
			}

		case reflect.Slice:
			err := mapSliceToStruct(rValue.Field(i), registerName, data)

			if err != nil {
				return err
			}

		case reflect.Map:
			if subMap, ok := data.Maps[registerName]; ok {
				err := mapMaptoMap(rValue.Field(i), subMap)

				if err != nil {
					return err
				}
			}

		case reflect.Struct:
			if subMap, ok := data.Maps[registerName]; ok {
				f := rValue.Field(i)

				newPath := append(path, registerName)

				err := mapToStruct(subMap, f, f.Type(), riakContext, newPath, riakRequest)

				if err != nil {
					return err
				}
			}

		case reflect.Ptr:

			f := rValue.Field(i)

			switch f.Type().String() {
			case "*goriak.Counter":
				var counterValue int64

				if val, ok := data.Counters[registerName]; ok {
					counterValue = val
				}

				resCounter := &Counter{
					name: registerName,
					path: path,
					key:  riakRequest,

					val: counterValue,
				}

				f.Set(reflect.ValueOf(resCounter))

			case "*goriak.Set":

				var setValue [][]byte

				if val, ok := data.Sets[registerName]; ok {
					setValue = val
				}

				resSet := &Set{
					name:    registerName,
					path:    path,
					key:     riakRequest,
					context: riakContext,

					value: setValue,
				}

				f.Set(reflect.ValueOf(resSet))

			default:
				return errors.New("Unexpected ptr type: " + f.Type().String())
			}

		default:
			return errors.New("Unknown type: " + field.Type.Kind().String())

		}
	}

	return nil
}

func mapSliceToStruct(sliceValue reflect.Value, registerName string, data *riak.Map) error {

	switch sliceValue.Type().Elem().Kind() {
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

	case reflect.String:
		if setVal, ok := data.Sets[registerName]; ok {
			result := make([]string, len(setVal))

			for i, v := range setVal {
				result[i] = string(v)
			}

			// Success!
			sliceValue.Set(reflect.ValueOf(result))
		}

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

func mapMaptoMap(mapValue reflect.Value, data *riak.Map) error {

	mapKeyType := mapValue.Type().Key().Kind()
	mapValueType := mapValue.Type().Elem().Kind()

	// Initialize the map
	newMap := reflect.MakeMap(mapValue.Type())
	mapValue.Set(newMap)

	for key, val := range data.Registers {

		// Convert key (a string) to the correct reflect.Value
		var keyValue reflect.Value
		switch mapKeyType {
		case reflect.String:
			keyValue = reflect.ValueOf(key)

		case reflect.Int:
			i, _ := strconv.ParseInt(key, 10, 0)
			keyValue = reflect.ValueOf(int(i))

		case reflect.Int8:
			i, _ := strconv.ParseInt(key, 10, 8)
			keyValue = reflect.ValueOf(int8(i))

		case reflect.Int16:
			i, _ := strconv.ParseInt(key, 10, 16)
			keyValue = reflect.ValueOf(int16(i))

		case reflect.Int32:
			i, _ := strconv.ParseInt(key, 10, 32)
			keyValue = reflect.ValueOf(int32(i))

		case reflect.Int64:
			i, _ := strconv.ParseInt(key, 10, 64)
			keyValue = reflect.ValueOf(int64(i))

		default:
			return errors.New("Unknown map key type")
		}

		// Convert val ([]byte) to the correct reflect.Value
		var valValue reflect.Value

		switch mapValueType {
		case reflect.String:
			valValue = reflect.ValueOf(string(val))

		case reflect.Slice:

			sliceItemType := mapValue.Type().Elem().Elem().Kind()

			switch sliceItemType {
			case reflect.Uint8:
				valValue = reflect.ValueOf(val)
			default:
				return errors.New("Unknown map value type")
			}

		default:
			return errors.New("Unknown map value type")
		}

		// Save value to the Go map
		mapValue.SetMapIndex(keyValue, valValue)
	}

	return nil
}
