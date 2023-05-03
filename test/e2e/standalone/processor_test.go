/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	"github.com/oracle/coherence-go-client/coherence/extractors"
	"github.com/oracle/coherence-go-client/coherence/filters"
	"github.com/oracle/coherence-go-client/coherence/processors"
	. "github.com/oracle/coherence-go-client/test/utils"
	"testing"
)

// TestProcessorAgainstMapAndCache runs all processor against NamedMap and NamedCache
func TestProcessorAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Person]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Person])
	}{
		{"NamedMapRunTestInvokeIncrement", GetNamedMap[int, Person](g, session, "increment-map"), RunTestInvokeIncrement},
		{"NamedCacheRunTestInvokeIncrement", GetNamedCache[int, Person](g, session, "increment-cache"), RunTestInvokeIncrement},
		{"NamedMapRunTestInvokeMultiply", GetNamedMap[int, Person](g, session, "multiply-map"), RunTestInvokeMultiply},
		{"NamedCacheRunTestInvokeMultiply", GetNamedCache[int, Person](g, session, "multiply-cache"), RunTestInvokeMultiply},
		{"NamedMapRunTestInvokeConditionalRemove", GetNamedMap[int, Person](g, session, "cond-remove-map"), RunTestInvokeConditionalRemove},
		{"NamedCacheRunTestInvokeConditionalRemove", GetNamedCache[int, Person](g, session, "cond-remove-cache"), RunTestInvokeConditionalRemove},
		{"NamedMapRunTestInvokeConditionalPut", GetNamedMap[int, Person](g, session, "cond-put-map"), RunTestInvokeConditionalPut},
		{"NamedCacheRunTestInvokeConditionalPut", GetNamedCache[int, Person](g, session, "cond-put-cache"), RunTestInvokeConditionalPut},
		{"NamedMapRunTestExtractProcessor", GetNamedMap[int, Person](g, session, "extractor-map"), RunTestExtractProcessor},
		{"NamedCacheRunTestExtractProcessor", GetNamedCache[int, Person](g, session, "extractor-cache"), RunTestExtractProcessor},
		{"NamedMapRunTestInvokeUpdater", GetNamedMap[int, Person](g, session, "updater-map"), RunTestInvokeUpdater},
		{"NamedCacheRunTestInvokeUpdater", GetNamedCache[int, Person](g, session, "updater-cache"), RunTestInvokeUpdater},
		{"NamedMapRunTestMethodInvocationProcessor", GetNamedMap[int, Person](g, session, "mip-map"), RunTestMethodInvocationProcessor},
		{"NamedCacheRunTestMethodInvocationProcessor", GetNamedCache[int, Person](g, session, "mip-cache"), RunTestMethodInvocationProcessor},
		{"NamedMapRunTestMethodInvocationProcessorMutator", GetNamedMap[int, Person](g, session, "mip-mutate-map"), RunTestMethodInvocationProcessorMutator},
		{"NamedCacheRunTestMethodInvocationProcessorMutator", GetNamedCache[int, Person](g, session, "mip-mutate-cache"), RunTestMethodInvocationProcessorMutator},
		{"NamedMapRunTestInvokeConditionalPutAll", GetNamedMap[int, Person](g, session, "map-conditional-put-all"), RunTestInvokeConditionalPutAll},
		{"NamedCacheRunTestInvokeConditionalPutAll", GetNamedCache[int, Person](g, session, "cache-conditional-put-all"), RunTestInvokeConditionalPutAll},
		{"NamedMapRunTestInvokeAll", GetNamedMap[int, Person](g, session, "map-invoke-all"), RunTestInvokeAll},
		{"NamedCacheRunTestInvokeAll", GetNamedCache[int, Person](g, session, "cache-invoke-all"), RunTestInvokeAll},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

// TestWithVersionedAgainstMapAndCache runs all tests that require VersionedPerson
func TestWithVersionedAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, VersionedPerson]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, VersionedPerson])
	}{
		{"NamedMapRunTestVersionedPut", GetNamedMap[int, VersionedPerson](g, session, "versioned-put-map"), RunTestVersionedPut},
		{"NamedCacheRunTestVersionedPut", GetNamedCache[int, VersionedPerson](g, session, "versioned-put-cache"), RunTestVersionedPut},
		{"NamedMapRunTestVersionedPutAll", GetNamedMap[int, VersionedPerson](g, session, "versioned-putall-map"), RunTestVersionedPutAll},
		{"NamedCacheRunTestVersionedPutAll", GetNamedCache[int, VersionedPerson](g, session, "versioned-putall-cache"), RunTestVersionedPutAll},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

// TestAgainstIntAndString runs all tests that require and int and string
func TestAgainstIntAndString(t *testing.T) {
	g := gomega.NewWithT(t)

	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, string]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, string])
	}{
		{"NamedMapRunTestPreloadProcessor", GetNamedMap[int, string](g, session, "preload"), RunTestPreloadProcessor},
		{"NamedCacheRunTestPreloadProcessor", GetNamedCache[int, string](g, session, "preload"), RunTestPreloadProcessor},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunTestPreloadProcessor(t *testing.T, namedMap coherence.NamedMap[int, string]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		value *string
	)

	err = namedMap.Clear(ctx)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))

	AssertSize(g, namedMap, 0)

	// Preload will cause the cache store to load the value of "Number 1"
	_, err = coherence.Invoke[int, string, string](ctx, namedMap, 1, processors.Preload())
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// value should be in cache
	AssertSize(g, namedMap, 1)

	value, err = namedMap.Get(ctx, 1)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(*value).To(gomega.Equal("Number 1"))
}

func RunTestMethodInvocationProcessor(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		value *string
	)

	addPerson(g, namedMap)

	// Preload will cause the cache store to load the value of "Number 1"
	value, err = coherence.Invoke[int, Person, string](ctx, namedMap, 1, processors.InvokeAccessor("toString"))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(value).To(gomega.Not(gomega.BeNil()))
}

func RunTestMethodInvocationProcessorMutator(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g     = gomega.NewWithT(t)
		err   error
		value *int
	)

	addPerson(g, namedMap)

	// Preload will cause the cache store to load the value of "Number 1"
	value, err = coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.InvokeMutator("remove", "age"))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(10))
}

func RunTestVersionedPut(t *testing.T, namedMap coherence.NamedMap[int, VersionedPerson]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		result   *VersionedPerson
		current  *VersionedPerson
		person   = VersionedPerson{ID: 1, Name: "Tim", Age: 10, Salary: 1000, Version: 1}
		oldValue *VersionedPerson
	)

	_, err = coherence.Invoke[int, VersionedPerson, bool](ctx, namedMap, 1,
		processors.VersionedPut(person, true, false))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Version).To(gomega.Equal(2))

	// update the name and keep version the same which should cause update
	result.Name = "New Name"

	_, err = coherence.Invoke[int, VersionedPerson, bool](ctx, namedMap, 1, processors.VersionedPut(result, false, false))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result.Name).To(gomega.Equal("New Name"))
	g.Expect(result.Version).To(gomega.Equal(3))

	// change the version to a version other than
	version := result.Version + 1
	result.Name = "This name will not be updated"
	result.Version = version

	current, err = coherence.Invoke[int, VersionedPerson, VersionedPerson](ctx, namedMap, 1,
		processors.VersionedPut(result, false, true))
	g.Expect(err).Should(gomega.BeNil())
	g.Expect(current.Name).To(gomega.Equal("New Name"))

	// value should not have been updated as version different
	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result.Name).To(gomega.Equal("New Name"))
	g.Expect(result.Version).To(gomega.Equal(3))
}

func RunTestVersionedPutAll(t *testing.T, namedMap coherence.NamedMap[int, VersionedPerson]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		values = map[int]VersionedPerson{
			1: {ID: 1, Name: "Tim", Age: 10, Version: 1},
			2: {ID: 2, Name: "Andrew", Age: 20, Version: 1},
			3: {ID: 3, Name: "John", Age: 30, Version: 1},
			4: {ID: 4, Name: "Steve", Age: 40, Version: 1},
		}
	)

	_, err = coherence.Invoke[int, VersionedPerson, bool](ctx, namedMap, 1,
		processors.VersionedPutAll(values, true, false))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	AssertSize(g, namedMap, 1)
}

func RunTestExtractProcessor(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		v        *string
		v2       *interface{}
		vAddress *Address
	)

	addPerson(g, namedMap)

	// ChainedExtractor
	chainedExtractor := processors.Extractor[string]("homeAddress.city")
	v, err = coherence.Invoke[int, Person, string](ctx, namedMap, 1, chainedExtractor)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(*v).To(gomega.Equal("Perth"))

	// extract the address
	proc := processors.Extractor[Address]("homeAddress")
	vAddress, err = coherence.Invoke[int, Person, Address](ctx, namedMap, 1, proc)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(vAddress.Address1).To(gomega.Equal("address1"))

	// multiple extractors
	proc = processors.Extractor[string]("name").
		AndThen(processors.Extractor[int]("age")).
		AndThen(processors.Extractor[float32]("salary"))

	v2, err = coherence.Invoke[int, Person, interface{}](ctx, namedMap, 1, proc)

	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(*v2).To(gomega.Equal([]interface{}{"Tim", float64(10), float64(1000)}))

	proc = processors.Extractor[string]("name")
	v, err = coherence.Invoke[int, Person, string](ctx, namedMap, 1, proc)
	g.Expect(err).To(gomega.Not(gomega.HaveOccurred()))
	g.Expect(*v).To(gomega.Equal("Tim"))
}

func RunTestInvokeIncrement(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		result *Person
		v      *int
	)

	addPerson(g, namedMap)

	v, err = coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.Increment("age", 1, true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// postInc == true which means return the value before it was incremented
	g.Expect(*v).To(gomega.Equal(int(10)))

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Age).To(gomega.Equal(11))

	v, err = coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.Increment("age", 1))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// postInc == false which means return the value before it was incremented
	g.Expect(*v).To(gomega.Equal(int(12)))

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Age).To(gomega.Equal(12))
}

func RunTestInvokeMultiply(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		result *Person
		v      *float32
	)

	addPerson(g, namedMap)

	v, err = coherence.Invoke[int, Person, float32](ctx, namedMap, 1, processors.Multiply("salary", 1.1, true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// postInc == true which means return the value before it was multiplied
	g.Expect(*v).To(gomega.Equal(float32(1000)))

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Salary).To(gomega.Equal(float32(1100)))

	v, err = coherence.Invoke[int, Person, float32](ctx, namedMap, 1, processors.Multiply("salary", 2.0))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// postInc == false which means return the value after it was multiplied
	g.Expect(*v).To(gomega.Equal(float32(2200)))

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Salary).To(gomega.Equal(float32(2200)))
}

func RunTestInvokeConditionalRemove(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		current  *Person
		person   = Person{ID: 1, Name: "Tim", Age: 10, Salary: 1000}
		oldValue *Person
	)

	_, err = namedMap.Put(ctx, 1, person)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// try and remove an entry with never filter and return the value as it will not be removed
	current, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Never(), true))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*current).To(gomega.Equal(person))

	// this will return nil for value which means return value as it was removed
	oldValue, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Always()))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// should have been removed
	AssertSize(g, namedMap, 0)

	oldValue, err = namedMap.Put(ctx, 1, person)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	oldValue, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalRemove(filters.Greater(extractors.Extract[int]("age"), 5)))
	// nil which means no value returns
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// should have been removed as age is greater than 5
	AssertSize(g, namedMap, 0)

	oldValue, err = namedMap.Put(ctx, 1, person)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	oldValue, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1,
		processors.ConditionalRemove(filters.Greater(extractors.Extract[int]("age"), 10)))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())

	// should not been removed as age is NOT greater than 10
	AssertSize(g, namedMap, 1)
}

func RunTestInvokeConditionalPut(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g        = gomega.NewWithT(t)
		err      error
		person   = Person{ID: 1, Name: "Tim", Age: 10, Salary: 1000}
		oldValue *Person
	)

	// should put as filter is true
	oldValue, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalPut[Person](filters.Always(), person))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize(g, namedMap, 1)

	ClearNamedMap[int, Person](g, namedMap)

	// should put as filter is false
	oldValue, err = coherence.Invoke[int, Person, Person](ctx, namedMap, 1, processors.ConditionalPut[Person](filters.Never(), person))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(oldValue).To(gomega.BeNil())
	AssertSize(g, namedMap, 0)
}

func RunTestInvokeUpdater(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		result *Person
		value  *bool
	)

	addPerson(g, namedMap)

	value, err = coherence.Invoke[int, Person, bool](ctx, namedMap, 1, processors.Update("age", 20))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(*value).To(gomega.Equal(true))

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(result.Age).To(gomega.Equal(20))
}

func RunTestInvokeConditionalPutAll(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g      = gomega.NewWithT(t)
		err    error
		values = map[int]Person{
			2: {ID: 2, Name: "Andrew", Age: 20},
			1: {ID: 1, Name: "Tim", Age: 10},
			3: {ID: 3, Name: "John", Age: 30},
			4: {ID: 4, Name: "Steve", Age: 40},
		}
	)

	// should put as filter is true
	_, err = coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.ConditionalPutAll[int, Person](filters.Always(), values))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize(g, namedMap, 1)

	ClearNamedMap[int, Person](g, namedMap)

	// should put as filter is false
	_, err = coherence.Invoke[int, Person, int](ctx, namedMap, 1, processors.ConditionalPutAll[int, Person](filters.Never(), values))
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize(g, namedMap, 0)
}

func RunTestInvokeAll(t *testing.T, namedMap coherence.NamedMap[int, Person]) {
	var (
		g   = gomega.NewWithT(t)
		err error
	)

	err = namedMap.PutAll(ctx, peopleData)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	// Increment the age of every person
	ch := coherence.InvokeAll[int, Person, int](ctx, namedMap, processors.Increment("age", 1, true))
	results := make([]int, 0)
	for se := range ch {
		// Check the error
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())
		results = append(results, se.Value)
		g.Expect(se.Value).Should(gomega.BeNumerically(">", 0))
	}

	g.Expect(len(results)).To(gomega.Equal(len(peopleData)))

	// ensure all values were updated
	expectedAgesAfterUpdate := map[int]int{1: 34, 2: 45, 3: 21, 4: 13}

	ch2 := namedMap.EntrySetFilter(ctx, filters.Always())
	for se := range ch2 {
		g.Expect(se.Err).ShouldNot(gomega.HaveOccurred())

		// check that the updates ages are correct
		g.Expect(expectedAgesAfterUpdate[se.Key]).To(gomega.Equal(se.Value.Age))
	}
}

// addPerson adds a Person and asserts that the size is 1
func addPerson(g *gomega.WithT, namedMap coherence.NamedMap[int, Person]) {
	_, err := namedMap.Put(ctx, 1, Person{ID: 1, Name: "Tim", Age: 10, Salary: 1000,
		HomeAddress: Address{Address1: "address1", Address2: "address1", City: "Perth", State: "WA", PostCode: 6000}})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	AssertSize(g, namedMap, 1)
}
