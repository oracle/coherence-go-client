/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package perf

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"log"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

type Student struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
	Course  string `json:"course"`
	Country string `json:"country"`
}

type Config struct {
	Session  *coherence.Session
	Students coherence.NamedCache[int, Student]
}

type PerformanceResult struct {
	Executions int64
	TotalTime  int64
	MinTime    time.Duration
	MaxTime    time.Duration
}

var (
	ctx = context.Background()
	//nolint:gosec // just a test - have something consistent
	rnd    = rand.New(rand.NewSource(123_456_789))
	config = Config{}

	courseExtractor  = extractors.Extract[string]("course")
	countryExtractor = extractors.Extract[string]("country")
	idExtractor      = extractors.Extract[int]("id")
	mapResults       = make(map[string]*PerformanceResult)
)

const (
	maxStudents = 2_000_000
)

// The entry point for the test suite
func TestMain(m *testing.M) {
	var (
		err  error
		size int
	)

	log.Println("Connecting to cluster")
	config, err = InitializeCoherence(ctx, "coherence:///localhost:7574")
	if err != nil {
		errorAndExit("connecting to cluster", err)
	}

	err = config.Students.Clear(ctx)
	if err != nil {
		errorAndExit("unable to clear", err)
	}

	// Add Indexes
	err = coherence.AddIndex[int, Student](ctx, config.Students, courseExtractor, true)
	if err != nil {
		errorAndExit("unable to add index", err)
	}

	err = coherence.AddIndex[int, Student](ctx, config.Students, countryExtractor, true)
	if err != nil {
		errorAndExit("unable to add index", err)
	}

	err = coherence.AddIndex[int, Student](ctx, config.Students, idExtractor, true)
	if err != nil {
		errorAndExit("unable to add index", err)
	}

	log.Println("Populating cache with", maxStudents, "students")
	if populateCache(config.Students, maxStudents) != nil {
		errorAndExit("failed to populate cache", err)
	}

	size, err = config.Students.Size(ctx)
	if err != nil {
		errorAndExit("error", err)
	}
	if size != maxStudents {
		errorAndExit("count", fmt.Errorf("invalid number of students: %d", size))
	}
	log.Println("Cache size is", size)

	exitCode := m.Run()

	fmt.Printf("Tests completed with return code %d\n", exitCode)
	printResults()

	os.Exit(exitCode)
}

func printResults() {
	keys := make([]string, 0, len(mapResults))
	for k := range mapResults {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Sort keys
	sort.Strings(keys)
	log.Println()
	log.Println("RESULTS START")
	log.Println()
	log.Printf("%-30s %15s %15s %15s %15s %15s\n", "TEST", "TOTAL TIME", "EXECUTIONS", "MIN", "MAX", "AVERAGE")
	for _, k := range keys {
		v := mapResults[k]
		log.Printf("%-30s %15v %15d %15v %15v %15v\n", k, time.Duration(v.TotalTime), v.Executions,
			v.MinTime, v.MaxTime, time.Duration(v.TotalTime/v.Executions))
	}
	log.Println()
	log.Println("RESULTS END")
}

func errorAndExit(message string, err error) {
	fmt.Println(message, err)
	os.Exit(1)
}

func InitializeCoherence(ctx context.Context, address string) (Config, error) {
	var (
		err error
	)

	// create a new Session to the default gRPC port of 1408 using plain text
	config.Session, err = coherence.NewSession(ctx, coherence.WithPlainText(), coherence.WithAddress(address))
	if err != nil {
		return config, err
	}

	config.Students, err = coherence.GetNamedCache[int, Student](config.Session, "students")
	if err != nil {
		return config, err
	}

	return config, nil
}

var (
	courses   = []string{"C1", "C2", "C3", "C4"}
	countries = []string{"Australia", "USA", "Canada", "Mexico"}
)

func populateCache(cache coherence.NamedMap[int, Student], count int) error {
	var (
		buffer    = make(map[int]Student)
		err       error
		batchSize = 10_000
	)

	for i := 1; i <= count; i++ {
		buffer[i] = getRandomStudent(i)
		if i%batchSize == 0 {
			err = cache.PutAll(ctx, buffer)
			if err != nil {
				return err
			}
			buffer = make(map[int]Student)
		}
	}
	if len(buffer) > 0 {
		return cache.PutAll(ctx, buffer)
	}

	return nil
}

func getRandomStudent(i int) Student {
	return Student{
		ID:      i,
		Name:    fmt.Sprintf("student%d", i),
		Address: fmt.Sprintf("address%d", i),
		Country: randomize(countries),
		Course:  randomize(courses),
	}
}

func randomize(arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[rnd.Intn(len(arr))]
}

type testTimer struct {
	startTime    time.Time
	endTime      time.Time
	minDuration  time.Duration
	maxDuration  time.Duration
	currentStart time.Time
	count        int64
}

func (t *testTimer) Start() {
	t.currentStart = time.Now()
}

func (t *testTimer) End() {
	duration := time.Since(t.currentStart)
	if duration < t.minDuration {
		t.minDuration = duration * time.Nanosecond
	}
	if duration > t.maxDuration {
		t.maxDuration = duration * time.Nanosecond
	}
}

func (t *testTimer) Complete() *PerformanceResult {
	t.endTime = time.Now()
	return &PerformanceResult{
		Executions: t.count,
		TotalTime:  t.endTime.Sub(t.startTime).Nanoseconds(),
		MaxTime:    t.maxDuration,
		MinTime:    t.minDuration,
	}
}

func newTestTimer(count int64) *testTimer {
	return &testTimer{
		startTime:   time.Now(),
		count:       count,
		minDuration: time.Duration(10000) * time.Second,
		maxDuration: 0,
	}
}
