/*
* Copyright (c) 2022, 2025 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"reflect"
	"testing"
)

func TestInvalidSerializer(t *testing.T) {
	serializer := NewSerializer[string]("invalid")

	serialized, err := serializer.Serialize("AAA")
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	deserialized, err := serializer.Deserialize(serialized)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if *deserialized != "AAA" {
		t.Fatalf("expected 'AAA', got '%s'", *deserialized)
	}
}

func TestJsonSerializer(t *testing.T) {
	type person struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	var myMap = map[int]person{
		1: {1, "tim"},
		2: {3, "tim2"},
	}

	testSerialization(t, "hello")
	testSerialization(t, 123)
	testSerialization(t, 123.123)
	testSerialization(t, int64(23))
	testSerialization(t, false)
	testSerialization(t, true)
	testSerialization(t, byte(1))
	testSerialization(t, []byte{1, 2, 3, 4})
	testSerialization(t, person{ID: 1, Name: "tim"})
	testSerialization(t, []string{"hello", "hello2", "hello3"})
	testSerialization(t, []int{12, 12, 12, 4, 4, 4, 3, 5})
	testSerialization(t, myMap)
}

func testSerialization[V any](t *testing.T, v V) {
	serializer := NewSerializer[V]("json")
	if serializer == nil {
		t.Fatal("expected serializer to be non-nil")
	}

	value, err := serializer.Serialize(v)
	if err != nil {
		t.Fatalf("Serialize failed for value %#v: %v", v, err)
	}

	finalValue, err := serializer.Deserialize(value)
	if err != nil {
		t.Fatalf("Deserialize failed for value %#v: %v", v, err)
	}

	if !reflect.DeepEqual(*finalValue, v) {
		t.Fatalf("expected deserialized value %#v to equal original %#v", *finalValue, v)
	}
}
