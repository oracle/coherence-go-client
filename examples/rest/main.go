/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main starts a listener on port localhost:17268 which provides a basic REST API providing POST, GET, PUT and DELETE operations against a NamedMap.

1.  Return all people

	$ curl -X GET -i http://localhost:17268/people
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 01:25:15 GMT
	Content-Length: 378
	Content-Type: text/plain; charset=utf-8

	[{"id":5,"name":"Person-5","address":"Address 5","city":"Adelaide","age":20},{"id":1,"name":"Person-1","address":"Address 1","city":"Adelaide","age":16},
	 {"id":3,"name":"Person-3","address":"Address 3","city":"Melbourne","age":18},{"id":4,"name":"Person-4","address":"Address 4","city":"Perth","age":19},
	 {"id":2,"name":"Person-2","address":"Address 2","city":"Sydney","age":17}]

2.  Return an individual person

	$ curl -X GET -i http://localhost:17268/people/1
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 01:25:25 GMT
	Content-Length: 76
	Content-Type: text/plain; charset=utf-8

	{"id":1,"name":"Person-1","address":"Address 1","city":"Adelaide","age":16}

3.  Remove person 1

	$ curl -X DELETE -i http://localhost:17268/people/1
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 02:46:35 GMT
	Content-Length: 0

	$ curl -X GET -i http://localhost:17268/people/1
	HTTP/1.1 404 Not Found
	Date: Mon, 24 Apr 2023 02:46:43 GMT
	Content-Length: 0

4.  Create a new person

	curl -X POST -i http://localhost:17268/people/1 -d '{"id":1,"name":"Person-1","address":"Address 1","city":"Adelaide","age":16}'
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 02:48:01 GMT
	Content-Length: 0

5. 	Update person 1

	$ curl -X PUT -i http://localhost:17268/people/1 -d '{"id":1,"name":"Person-1","address":"Address 1","city":"Singapore","age":16}'
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 02:48:33 GMT
	Content-Length: 0

	$ curl -X GET -i http://localhost:17268/people/1
	HTTP/1.1 200 OK
	Date: Mon, 24 Apr 2023 02:48:37 GMT
	Content-Length: 77
	Content-Type: text/plain; charset=utf-8
*/
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Person struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
	City    string `json:"city"`
	Age     int    `json:"age"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, address=%s, city=%s, age=%d}", p.ID, p.Name, p.Address, p.City, p.Age)
}

var (
	namedMap coherence.NamedMap[int, Person]
	ctx      = context.TODO()
)

func main() {
	// create a new Session to the default gRPC port of 1408 using plain text
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		log.Println("unable to connect to Coherence", err)
		return
	}
	defer session.Close()

	namedMap, err = coherence.NewNamedMap[int, Person](session, "people")
	if err != nil {
		log.Println("unable to create namedMap 'people'", err)
		return
	}

	// Populate the cache
	populateCoherence()

	// Add routes
	http.HandleFunc("/people", getAllPeople)
	http.HandleFunc("/people/", handlePersonRequest)
	fmt.Println("Listening on http://localhost:17268/, press CTRL-C to stop.")

	server := &http.Server{
		Addr:              "localhost:17268",
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err = server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// populateCoherence populates Coherence with sample data.
func populateCoherence() {
	cities := []string{"Perth", "Adelaide", "Sydney", "Melbourne"}
	buffer := make(map[int]Person)

	if err := namedMap.Clear(ctx); err != nil {
		panic(err)
	}

	for i := 1; i <= 5; i++ {
		p := Person{ID: i, Name: fmt.Sprintf("Person-%d", i), Age: 15 + i%40,
			Address: fmt.Sprintf("Address %d", i), City: cities[i%4]}
		buffer[p.ID] = p
	}
	if err := namedMap.PutAll(ctx, buffer); err != nil {
		panic(err)
	}
}

// getAllPeople returns all people in the NamedMap.
func getAllPeople(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		var people = make([]Person, 0)

		for ch := range namedMap.Values(ctx) {
			if ch.Err != nil {
				panic(ch.Err)
			}
			people = append(people, ch.Value)
		}
		_ = json.NewEncoder(w).Encode(people)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

// handlePersonRequest handles a GET, PUT, DELETE or POST request.
func handlePersonRequest(w http.ResponseWriter, r *http.Request) {
	log.Println("Request", r.Method, r.URL.Path)
	if r.Method == http.MethodGet {
		handleGETPerson(w, r)
	} else if r.Method == http.MethodPost {
		handlePUTPerson(w, r)
	} else if r.Method == http.MethodDelete {
		handleDELETEPerson(w, r)
	} else if r.Method == http.MethodPut {
		handlePUTPerson(w, r)
	}
}

// handleGETPerson handles a GET request for a specific person and return 404 if the person
// is not found or 400 if an invalid person id is supplied.
func handleGETPerson(w http.ResponseWriter, r *http.Request) {
	id, err := getPersonID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}

	person, err := namedMap.Get(ctx, id)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}

	if person == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_ = json.NewEncoder(w).Encode(person)
}

// handlePUTPerson handles a PUT or POST request for a specific person and will Put the value
// into the Coherence NamedMap.
func handlePUTPerson(w http.ResponseWriter, r *http.Request) {
	var person Person
	id, err := getPersonID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(body, &person)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}

	// check the path id == person ID
	if person.ID != id {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err = namedMap.Put(ctx, person.ID, person)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
}

// handleGETPerson handles a DELETE request for a specific person and returns 404 if the person
// is not found or 400 if an invalid person id is supplied.
func handleDELETEPerson(w http.ResponseWriter, r *http.Request) {
	id, err := getPersonID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}

	_, err = namedMap.Remove(ctx, id)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
}

// getPersonID returns the person ID from the path.
func getPersonID(r *http.Request) (int, error) {
	personID := strings.TrimPrefix(r.URL.Path, "/people/")
	id, err := strconv.Atoi(personID)
	return id, err
}
