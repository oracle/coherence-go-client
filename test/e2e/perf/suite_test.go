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
	"log"
	"math/rand"
	"os"
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

var (
	ctx = context.Background()
	//nolint:gosec // just a test
	rnd    = rand.New(rand.NewSource(time.Now().UnixNano()))
	config = Config{}
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

	log.Println("Populating cache with", maxStudents, "students")
	if populateCache(config.Students, maxStudents) != nil {
		errorAndExit("failed to populate cache", err)
	}

	size, err = config.Students.Size(ctx)
	if err != nil || size != maxStudents {
		errorAndExit("size not correct", err)
	}
	log.Println("Cache size is", size)

	exitCode := m.Run()

	fmt.Printf("Tests completed with return code %d\n", exitCode)

	os.Exit(exitCode)
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

func populateCache(cache coherence.NamedMap[int, Student], count int) error {
	var (
		buffer    = make(map[int]Student)
		err       error
		courses   = []string{"C1", "C2", "C3", "C4"}
		countries = []string{"Australia", "USA", "Canada", "Mexico"}
		batchSize = 10_000
	)

	for i := 1; i <= count; i++ {
		buffer[i] = Student{
			ID:      i,
			Name:    fmt.Sprintf("student%d", i),
			Address: fmt.Sprintf("address%d", i),
			Country: randomize(countries),
			Course:  randomize(courses),
		}
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

func randomize(arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[rnd.Intn(len(arr))]
}
