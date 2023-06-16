/*
 * Copyright (c) 2022, 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

/*
Package main listens for all events on a NamedMap or NamedCache.
*/
package main

import (
	"context"
	"fmt"
	"github.com/oracle/coherence-go-client/coherence"
	"log"
	"time"
)

type Person struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Age        int    `json:"age"`
	InsertTime int64  `json:"insertTime"`
}

func (p Person) String() string {
	return fmt.Sprintf("Person{id=%d, name=%s, age=%d, insertTime=%v}", p.ID, p.Name, p.Age, p.InsertTime)
}

func main() {
	ctx := context.Background()

	// create a new Session
	session, err := coherence.NewSession(ctx, coherence.WithPlainText())
	if err != nil {
		panic(err)
	}

	defer session.Close()

	// create a new NamedMap of Person with key int
	namedMap, err := coherence.GetNamedMap[int, Person](session, "people")
	if err != nil {
		panic(err)
	}

	fmt.Println("Adding listener for all events")
	listener := coherence.NewMapListener[int, Person]().OnAny(func(e coherence.MapEvent[int, Person]) {
		var (
			newValue *Person
			oldValue *Person
		)
		key, err1 := e.Key()
		if err1 != nil {
			panic("unable to deserialize key")
		}

		if e.Type() == coherence.EntryInserted || e.Type() == coherence.EntryUpdated {
			newValue, err1 = e.NewValue()
			if err1 != nil {
				panic("unable to deserialize new value")
			}
		}
		if e.Type() == coherence.EntryDeleted || e.Type() == coherence.EntryUpdated {
			oldValue, err1 = e.OldValue()
			if err1 != nil {
				panic("unable to deserialize old value")
			}
		}

		log.Printf("***EVENT=%v: key=%v, oldValue=%v, newValue=%v\n", e.Type(), *key, oldValue, newValue)
	})

	if err = namedMap.AddListener(ctx, listener); err != nil {
		panic(err)
	}

	defer func(ctx context.Context, namedMap coherence.NamedMap[int, Person], listener coherence.MapListener[int, Person]) {
		if err := namedMap.RemoveListener(ctx, listener); err != nil {
			panic(fmt.Sprintf("cannot remove listener %v, %v", listener, err))
		}
	}(ctx, namedMap, listener)

	time.Sleep(time.Duration(10000000) * time.Second)
}
