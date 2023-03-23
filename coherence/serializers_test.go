/*
* Copyright (c) 2022, 2023 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"github.com/onsi/gomega"
	"testing"
)

func TestInvalidSerializer(t *testing.T) {
	g := gomega.NewWithT(t)

	// test that the default format of "json" will always be used
	serializer := NewSerializer[string]("invalid")

	serialized, err := serializer.Serialize("AAA")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	deserialized, err := serializer.Deserialize(serialized)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*deserialized).To(gomega.Equal("AAA"))
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

	testSerialization[string](t, "hello")
	testSerialization[int](t, 123)
	testSerialization[float64](t, 123.123)
	testSerialization[int64](t, 23)
	testSerialization[bool](t, false)
	testSerialization[bool](t, true)
	testSerialization[byte](t, 1)
	testSerialization[[]byte](t, []byte{1, 2, 3, 4})
	testSerialization[person](t, person{ID: 1, Name: "tim"})
	testSerialization[[]string](t, []string{"hello", "hello2", "hello3"})
	testSerialization[[]int](t, []int{12, 12, 12, 4, 4, 4, 3, 5})
	testSerialization[map[int]person](t, myMap)
}

func testSerialization[V any](t *testing.T, v V) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	serializer := NewSerializer[V]("json")
	g.Expect(serializer).To(gomega.Not(gomega.BeNil()))

	value, err := serializer.Serialize(v)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	finaValue, err := serializer.Deserialize(value)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*finaValue).To(gomega.Equal(v))
}
