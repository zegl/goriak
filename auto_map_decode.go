package goriak

import (
	"errors"
	riak "github.com/basho/riak-go-client"
	"reflect"
	"strconv"
)

func mapToStruct(data *riak.Map, output interface{}) error {

	// Set values
	rValue := reflect.ValueOf(output).Elem()
	rType := reflect.TypeOf(output).Elem()
	num := rType.NumField()

	for i := 0; i < num; i++ {

		field := rType.Field(i)
		registerName := field.Name
		tag := field.Tag.Get("goriak")

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

		case reflect.Int:

			if val, ok := data.Registers[registerName]; ok {
				intVal, err := strconv.Atoi(string(val))

				if err == nil {
					rValue.Field(i).SetInt(int64(intVal))
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
					panic(err)
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
