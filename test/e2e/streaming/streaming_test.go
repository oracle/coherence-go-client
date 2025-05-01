/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package streaming

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/aggregators"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	testTypeEntrySet       = "entrySet"
	testTypeEntrySetFilter = "entrySetFilter"
	testTypeKeySet         = "keySet"
	testTypeKeySetFilter   = "keySetFilter"
	testTypeValues         = "values"
	testTypeValuesFilter   = "valuesFilter"

	defaultCacheCount = 200_000
)

var (
	rnd              = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	extractor        = extractors.Extract[string]("department")
	filterIT         = filters.Equal[string](extractor, "IT")
	ctx              = context.Background()
	actualCacheCount = defaultCacheCount
)

// TestStreaming runs various streaming concurrent tests to ensure that all the streaming
// operations are thread safe.
func TestStreamingConcurrency(t *testing.T) {
	g := gomega.NewWithT(t)

	_ = os.Setenv("COHERENCE_CLIENT_REQUEST_TIMEOUT", "300000")

	osCacheCount := os.Getenv("COHERENCE_CACHE_COUNT")
	if os.Getenv(osCacheCount) != "" {
		v, err := strconv.Atoi(osCacheCount)
		if err != nil {
			actualCacheCount = v
		}
	}

	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	cache := utils.GetNamedMap[int, utils.Person](g, session, "streaming-people")

	// clear and re-populate
	g.Expect(cache.Clear(ctx)).ShouldNot(gomega.HaveOccurred())

	g.Expect(coherence.AddIndex[int, utils.Person](ctx, cache, extractor, true)).ShouldNot(gomega.HaveOccurred())

	g.Expect(populateCache(cache, actualCacheCount)).ShouldNot(gomega.HaveOccurred())

	// get the count of people in IT
	itCount, err := coherence.AggregateFilter[int, utils.Person](ctx, cache, filterIT, aggregators.Count())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	size, err := cache.Size(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(size).To(gomega.Equal(actualCacheCount))

	log.Println("Cache Size", size)

	testCases := []struct {
		testName string
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Person], testType string, expectedITCount int64, fltr filters.Filter)
		testType string
		fltr     filters.Filter
	}{
		{getTestName("FilterIT", testTypeEntrySetFilter), RunTestStreaming, testTypeEntrySetFilter, filterIT},
		{getTestName("NoFilter", testTypeEntrySet), RunTestStreaming, testTypeEntrySet, nil},
		{getTestName("FilterIT", testTypeKeySetFilter), RunTestStreaming, testTypeKeySetFilter, filterIT},
		{getTestName("NoFilter", testTypeKeySet), RunTestStreaming, testTypeKeySet, nil},
		{getTestName("FilterIT", testTypeValuesFilter), RunTestStreaming, testTypeValuesFilter, filterIT},
		{getTestName("NoFilter", testTypeValues), RunTestStreaming, testTypeValues, nil},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, cache, tc.testType, *itCount, tc.fltr)
		})
	}
}

func getTestName(prefix, testType string) string {
	return fmt.Sprint("StreamingTest_", prefix, testType)
}

func RunTestStreaming(t *testing.T, namedMap coherence.NamedMap[int, utils.Person], testType string, expectedCount int64, fltr filters.Filter) {
	var (
		g           = gomega.NewWithT(t)
		wg          sync.WaitGroup
		threadCount = 20
	)

	if fltr == nil {
		// nil filter means we should get all entries
		expectedCount = int64(actualCacheCount)
	}

	log.Printf("testType=%s, expectedCount=%d\n", testType, expectedCount)

	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func(t int) {
			defer wg.Done()
			actualCount := runStreamingTest(g, namedMap, testType, fltr)
			log.Printf("thread=%d, expectedCount=%d, actualCount=%d\n", t, expectedCount, actualCount)
			g.Expect(actualCount).To(gomega.Equal(int(expectedCount)))
		}(i)
	}
	wg.Wait()
}

func runStreamingTest(g *gomega.WithT, namedMap coherence.NamedMap[int, utils.Person], testType string, fltr filters.Filter) int {
	var (
		count       = 0
		finalFilter = filters.Always()
		chEntry     <-chan *coherence.StreamedEntry[int, utils.Person]
		chKey       <-chan *coherence.StreamedKey[int]
		chValue     <-chan *coherence.StreamedValue[utils.Person]
	)

	if fltr != nil {
		finalFilter = fltr
	}

	if testType == testTypeEntrySetFilter {
		chEntry = namedMap.EntrySetFilter(ctx, finalFilter)
	} else if testType == testTypeEntrySet {
		chEntry = namedMap.EntrySet(ctx)
	} else if testType == testTypeKeySetFilter {
		chKey = namedMap.KeySetFilter(ctx, finalFilter)
	} else if testType == testTypeKeySet {
		chKey = namedMap.KeySet(ctx)
	} else if testType == testTypeValuesFilter {
		chValue = namedMap.ValuesFilter(ctx, finalFilter)
	} else if testType == testTypeValues {
		chValue = namedMap.Values(ctx)
	} else {
		g.Fail(fmt.Sprintf("unknown testType=%s", testType))
	}

	if testType == testTypeEntrySetFilter || testType == testTypeEntrySet {
		for ch := range chEntry {
			g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
			if ch.Err != nil {
				continue
			}
			_ = ch.Value
			count++
		}
		return count
	}

	if testType == testTypeKeySetFilter || testType == testTypeKeySet {
		for ch := range chKey {
			g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
			if ch.Err != nil {
				continue
			}
			_ = ch.Key
			count++
		}
		return count
	}

	if testType == testTypeValuesFilter || testType == testTypeValues {
		for ch := range chValue {
			g.Expect(ch.Err).ShouldNot(gomega.HaveOccurred())
			if ch.Err != nil {
				continue
			}
			_ = ch.Value
			count++
		}
		return count
	}

	return 0
}

func populateCache(cache coherence.NamedMap[int, utils.Person], count int) error {
	var (
		buffer      = make(map[int]utils.Person)
		err         error
		departments = []string{"IT", "HR", "FINANCE", "MARKETING", "DEV"}
		batchSize   = 1000
	)

	for i := 1; i <= count; i++ {
		buffer[i] = utils.Person{
			ID:         i,
			Name:       fmt.Sprintf("person%d", i),
			Age:        i%100 + 10,
			Department: randomize(departments),
		}
		if i%batchSize == 0 {
			err = cache.PutAll(ctx, buffer)
			if err != nil {
				return err
			}
			buffer = make(map[int]utils.Person)
		}
	}
	if len(buffer) > 0 {
		return cache.PutAll(ctx, buffer)
	}

	return nil
}

func randomize(arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[rnd.Intn(len(arr))]
}
