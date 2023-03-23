/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	jsonSerializationPrefix = 21
)

var (
	_           Serializer[string] = JSONSerializer[string]{"json"}
	conversions                    = [2]string{"\"@class\":\"math.BigDec\",", "\"@class\":\"math.BigInt\","}
)

// mathValue is used to extract the value of math.BigDec or math.BigInt
type mathValue[T any] struct {
	Value T `json:"value"`
}

// Serializer defines how to serialize/ de-serialize objects.
type Serializer[T any] interface {
	Serialize(object T) ([]byte, error)
	Deserialize(data []byte) (*T, error)
	Format() string
}

// NewSerializer returns a new Serializer based upon the format and the type.
func NewSerializer[T any](format string) Serializer[T] {
	// currently only "json" serialization is supported. If another
	// serialization format is used, it will default to "json".
	if format == "json" {
		return JSONSerializer[T]{format: "json"}
	}
	return JSONSerializer[T]{format: "json"}
}

// JSONSerializer serializes data using JSON.
type JSONSerializer[T any] struct {
	format string
}

// Serialize serializes an object of type T and returns the []byte representation.
func (s JSONSerializer[T]) Serialize(object T) ([]byte, error) {
	data, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}

	finalData := make([]byte, 1)
	finalData[0] = jsonSerializationPrefix
	finalData = append(finalData, data...)
	return finalData, nil
}

// Deserialize deserialized an object and returns the correct type of T.
func (s JSONSerializer[T]) Deserialize(data []byte) (*T, error) {
	var (
		finalData   []byte
		finalResult T
		err         error
		zeroValue   T
	)
	if len(data) == 0 {
		return nil, nil
	}

	if data[0] == jsonSerializationPrefix {
		// ensure we return the correct type of instance based upon the generic type
		finalResult = *new(T)
		finalData = append(finalData, data[1:]...)
		if string(finalData) == "null" {
			return nil, nil
		}

		// check for deserialization of math.BigDec or math.BigInt
		finalDataString := string(finalData)
		converted := false
		for _, v := range conversions {
			// if the serialized data contains the conversions then just remove them as the
			// serializer will sort them out
			if strings.Contains(finalDataString, v) {
				finalData = []byte(strings.Replace(finalDataString, v, "", 1))
				converted = true
			}
		}

		if converted {
			// JSON is in format of "value":"19.50000000" rather than just the "19.50000000"
			// so retrieve the string value
			var result mathValue[T]
			if err = json.Unmarshal(finalData, &result); err != nil {
				return nil, err
			}
			finalResult = result.Value
			return &finalResult, nil
		}

		err = json.Unmarshal(finalData, &finalResult)
		return &finalResult, err
	}

	return &zeroValue, fmt.Errorf("invalid serialization prefix %v", data[0])
}

func (s JSONSerializer[T]) Format() string {
	return s.format
}
