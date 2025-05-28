/*
 * Copyright (c) 2025, Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/extractors"
	"github.com/oracle/coherence-go-client/v2/coherence/filters"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"
)

type School struct {
	ID               int    `json:"id"`
	City             string `json:"city"`
	State            string `json:"state"`
	MarketSegment    string `json:"marketSegment"`
	CountryCode      string `json:"countryCode"`
	SchoolName       string `json:"schoolName"`
	LastModifiedDate string `json:"lastModifiedDate"` // "2006-01-02 15:04:05"
}

var (
	session      *coherence.Session
	schoolsCache coherence.NamedCache[int, School]

	marketSegmentExtractor = extractors.Extract[string]("marketSegment")
	marketSegmentFilter    = filters.Equal[string](marketSegmentExtractor, "PRIVATE")

	rnd = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(time.Now().UnixNano()))) //nolint:gosec // this is a test
)

func main() {
	var (
		count    = 250_000
		testType = "runSchoolsTest"
		err      error
	)
	if err = initializeCoherence(); err != nil {
		log.Fatalf("failed to initialize coherence: %v", err)
	}

	countOverride := os.Getenv("CACHE_COUNT")
	if countOverride != "" {
		count, _ = strconv.Atoi(countOverride)
	}

	if value := os.Getenv("TEST_TYPE"); value != "" {
		testType = value
	}

	if testType == "load" {
		err = populateSchoolsCache(count)
	} else {
		err = runSchoolsTest()
	}
	if err != nil {
		log.Fatalf("failed to run %s: %v", testType, err)
	}
}

func initializeCoherence() error {
	var err error
	session, err = coherence.NewSession(context.Background(), coherence.WithPlainText())
	if err != nil {
		return err
	}

	// add a reconnect listener
	listener := coherence.NewSessionLifecycleListener().
		OnDisconnected(func(e coherence.SessionLifecycleEvent) {
			log.Printf("Session [%s] disconnected\n", e.Type())
		})

	session.AddSessionLifecycleListener(listener)

	schoolsCache, err = coherence.GetNamedCache[int, School](session, "schools")

	return err
}

func runSchoolsTest() error {
	http.HandleFunc("/api/schools", schoolsHandler)

	fmt.Println("Server running on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil)) //nolint:gosec // G404: weak RNG

	return nil
}

func schoolsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	defer func() {
		if session.IsClosed() {
			log.Printf("Session [%s] is closed, calling initializeCoherence()", session.ID())
			err := initializeCoherence()
			if err != nil {
				log.Fatalf("failed to initialize coherence: %v", err)
			}
		}
	}()

	switch r.Method {

	case http.MethodGet:
		var (
			count int
			start = time.Now()
		)

		defer func() {
			duration := time.Since(start)
			log.Printf("Retrieved %d entries in %s", count, duration)
		}()

		var people = make([]School, 0)

		ready, err := schoolsCache.IsReady(ctx)
		if err != nil || !ready {
			session.Close()
			log.Printf("failed to do health check: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for ch := range schoolsCache.ValuesFilter(ctx, marketSegmentFilter) {
			if ch.Err != nil {
				http.Error(w, ch.Err.Error(), http.StatusInternalServerError)
				return
			}
			people = append(people, ch.Value)
			count++
		}
		_ = json.NewEncoder(w).Encode(people)
		return
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func populateSchoolsCache(count int) error {
	var (
		buffer         = make(map[int]School)
		err            error
		batchSize      = 10_000
		marketSegments = []string{"K-12", "MAGNET", "CHARTER", "VIRTUAL", "RELIGIOUS", "BOARDING", "PRIVATE"}
		ctx            = context.Background()
	)

	if err = schoolsCache.Clear(ctx); err != nil {
		return err
	}

	if err = coherence.AddIndex[int, School](ctx, schoolsCache, marketSegmentExtractor, true); err != nil {
		return err
	}

	for i := 1; i <= count; i++ {
		modifiedDate := time.Now().UTC().Format(time.DateTime)

		randomCity := randomUSCity()
		buffer[i] = School{
			ID:               i,
			City:             randomCity.City,
			State:            randomCity.State,
			MarketSegment:    randomize(marketSegments),
			CountryCode:      "US",
			SchoolName:       fmt.Sprintf("School Name %d", i),
			LastModifiedDate: modifiedDate,
		}
		if i%batchSize == 0 {
			err = schoolsCache.PutAll(ctx, buffer)
			if err != nil {
				return err
			}
			buffer = make(map[int]School)
			log.Println("PutAll", i)
		}
	}
	if len(buffer) > 0 {
		return schoolsCache.PutAll(ctx, buffer)
	}

	return nil
}

func randomize(arr []string) string {
	if len(arr) == 0 {
		return ""
	}
	return arr[rnd.IntN(len(arr))]
}

type USCity struct {
	City  string
	State string
}

var cities = []USCity{
	// California (CA)
	{"Los Angeles", "CA"}, {"San Diego", "CA"}, {"San Jose", "CA"}, {"San Francisco", "CA"}, {"Fresno", "CA"},
	{"Sacramento", "CA"}, {"Long Beach", "CA"}, {"Oakland", "CA"}, {"Bakersfield", "CA"}, {"Anaheim", "CA"},
	{"Santa Ana", "CA"}, {"Riverside", "CA"}, {"Stockton", "CA"}, {"Irvine", "CA"}, {"Chula Vista", "CA"},
	{"Fremont", "CA"}, {"San Bernardino", "CA"}, {"Modesto", "CA"}, {"Oxnard", "CA"}, {"Fontana", "CA"},

	// Texas (TX)
	{"Houston", "TX"}, {"San Antonio", "TX"}, {"Dallas", "TX"}, {"Austin", "TX"}, {"Fort Worth", "TX"},
	{"El Paso", "TX"}, {"Arlington", "TX"}, {"Corpus Christi", "TX"}, {"Plano", "TX"}, {"Laredo", "TX"},
	{"Lubbock", "TX"}, {"Garland", "TX"}, {"Irving", "TX"}, {"Amarillo", "TX"}, {"Grand Prairie", "TX"},
	{"McKinney", "TX"}, {"Frisco", "TX"}, {"Brownsville", "TX"}, {"Pasadena", "TX"}, {"Killeen", "TX"},

	// Florida (FL)
	{"Jacksonville", "FL"}, {"Miami", "FL"}, {"Tampa", "FL"}, {"Orlando", "FL"}, {"St. Petersburg", "FL"},
	{"Hialeah", "FL"}, {"Tallahassee", "FL"}, {"Fort Lauderdale", "FL"}, {"Port St. Lucie", "FL"}, {"Cape Coral", "FL"},
	{"Pembroke Pines", "FL"}, {"Hollywood", "FL"}, {"Miramar", "FL"}, {"Gainesville", "FL"}, {"Coral Springs", "FL"},
	{"Clearwater", "FL"}, {"Palm Bay", "FL"}, {"Pompano Beach", "FL"}, {"West Palm Beach", "FL"}, {"Lakeland", "FL"},

	// New York (NY)
	{"New York", "NY"}, {"Buffalo", "NY"}, {"Rochester", "NY"}, {"Yonkers", "NY"}, {"Syracuse", "NY"},
	{"Albany", "NY"}, {"New Rochelle", "NY"}, {"Mount Vernon", "NY"}, {"Schenectady", "NY"}, {"Utica", "NY"},
	{"White Plains", "NY"}, {"Troy", "NY"}, {"Niagara Falls", "NY"}, {"Binghamton", "NY"}, {"Freeport", "NY"},
	{"Valley Stream", "NY"}, {"Hempstead", "NY"}, {"Levittown", "NY"}, {"Brentwood", "NY"}, {"Irondequoit", "NY"},

	// Illinois (IL)
	{"Chicago", "IL"}, {"Aurora", "IL"}, {"Naperville", "IL"}, {"Joliet", "IL"}, {"Rockford", "IL"},
	{"Springfield", "IL"}, {"Elgin", "IL"}, {"Peoria", "IL"}, {"Champaign", "IL"}, {"Waukegan", "IL"},
	{"Cicero", "IL"}, {"Bloomington", "IL"}, {"Arlington Heights", "IL"}, {"Evanston", "IL"}, {"Decatur", "IL"},
	{"Schaumburg", "IL"}, {"Bolingbrook", "IL"}, {"Palatine", "IL"}, {"Skokie", "IL"}, {"Des Plaines", "IL"},

	// Pennsylvania (PA)
	{"Philadelphia", "PA"}, {"Pittsburgh", "PA"}, {"Allentown", "PA"}, {"Erie", "PA"}, {"Reading", "PA"},
	{"Scranton", "PA"}, {"Bethlehem", "PA"}, {"Lancaster", "PA"}, {"Harrisburg", "PA"}, {"Altoona", "PA"},
	{"York", "PA"}, {"State College", "PA"}, {"Wilkes-Barre", "PA"}, {"Norristown", "PA"}, {"Chester", "PA"},
	{"Bethel Park", "PA"}, {"Monroeville", "PA"}, {"Plum", "PA"}, {"Easton", "PA"}, {"Lebanon", "PA"},

	// Ohio (OH)
	{"Columbus", "OH"}, {"Cleveland", "OH"}, {"Cincinnati", "OH"}, {"Toledo", "OH"}, {"Akron", "OH"},
	{"Dayton", "OH"}, {"Parma", "OH"}, {"Canton", "OH"}, {"Youngstown", "OH"}, {"Lorain", "OH"},
	{"Hamilton", "OH"}, {"Springfield", "OH"}, {"Kettering", "OH"}, {"Elyria", "OH"}, {"Lakewood", "OH"},
	{"Cuyahoga Falls", "OH"}, {"Middletown", "OH"}, {"Euclid", "OH"}, {"Newark", "OH"}, {"Mansfield", "OH"},

	// Georgia (GA)
	{"Atlanta", "GA"}, {"Augusta", "GA"}, {"Columbus", "GA"}, {"Macon", "GA"}, {"Savannah", "GA"},
	{"Athens", "GA"}, {"Sandy Springs", "GA"}, {"Roswell", "GA"}, {"Johns Creek", "GA"}, {"Warner Robins", "GA"},
	{"Albany", "GA"}, {"Alpharetta", "GA"}, {"Marietta", "GA"}, {"Valdosta", "GA"}, {"Smyrna", "GA"},
	{"Dunwoody", "GA"}, {"Rome", "GA"}, {"Peachtree City", "GA"}, {"Gainesville", "GA"}, {"Brookhaven", "GA"},

	// North Carolina (NC)
	{"Charlotte", "NC"}, {"Raleigh", "NC"}, {"Greensboro", "NC"}, {"Durham", "NC"}, {"Winston-Salem", "NC"},
	{"Fayetteville", "NC"}, {"Cary", "NC"}, {"Wilmington", "NC"}, {"High Point", "NC"}, {"Greenville", "NC"},
	{"Asheville", "NC"}, {"Concord", "NC"}, {"Gastonia", "NC"}, {"Jacksonville", "NC"}, {"Chapel Hill", "NC"},
	{"Rocky Mount", "NC"}, {"Huntersville", "NC"}, {"Burlington", "NC"}, {"Wilson", "NC"}, {"Kannapolis", "NC"},

	// Michigan (MI)
	{"Detroit", "MI"}, {"Grand Rapids", "MI"}, {"Warren", "MI"}, {"Sterling Heights", "MI"}, {"Ann Arbor", "MI"},
	{"Lansing", "MI"}, {"Flint", "MI"}, {"Dearborn", "MI"}, {"Livonia", "MI"}, {"Westland", "MI"},
	{"Troy", "MI"}, {"Farmington Hills", "MI"}, {"Kalamazoo", "MI"}, {"Wyoming", "MI"}, {"Southfield", "MI"},
	{"Rochester Hills", "MI"}, {"Taylor", "MI"}, {"Pontiac", "MI"}, {"St. Clair Shores", "MI"}, {"Royal Oak", "MI"},
}

func randomUSCity() USCity {
	return cities[rand.IntN(len(cities))] //nolint:gosec // this is a test,
}
