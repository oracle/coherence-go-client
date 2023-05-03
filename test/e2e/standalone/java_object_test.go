/*
 * Copyright (c) 2023 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/coherence"
	. "github.com/oracle/coherence-go-client/test/utils"
	"testing"
)

type Customer struct {
	Class              string          `json:"@class"`
	ID                 int             `json:"id"`
	CustomerName       string          `json:"customerName"`
	HomeAddress        CustomerAddress `json:"homeAddress"`
	PostalAddress      CustomerAddress `json:"postalAddress"`
	CustomerType       string          `json:"customerType"`
	OutstandingBalance float32         `json:"outstandingBalance"`
}

type CustomerAddress struct {
	Class        string `json:"@class"`
	AddressLine1 string `json:"addressLine1"`
	AddressLine2 string `json:"addressLine2"`
	Suburb       string `json:"suburb"`
	City         string `json:"city"`
	State        string `json:"state"`
	PostCode     int    `json:"postCode"`
}

// TestBasicOperationsAgainstMapAndCache runs all tests against NamedMap and NamedCache
func TestJavaSerializationAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, Customer]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, Customer])
	}{
		{"NamedMapSerializationTest", GetNamedMap[int, Customer](g, session, "customer-map"), RunSerializationTest},
		{"NamedCacheSerializationTest", GetNamedCache[int, Customer](g, session, "customer-cache"), RunSerializationTest},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunSerializationTest(t *testing.T, namedMap coherence.NamedMap[int, Customer]) {
	var (
		g      = gomega.NewWithT(t)
		result *Customer
		err    error
	)

	// these must be defined in META-INF/type-aliases.properties on the server like:
	// test.customer=com.oracle.coherence.go.testing.Customer
	// test.address=com.oracle.coherence.go.testing.Address
	const customerClass = "test.customer"
	const addressClass = "test.address"

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	homeAddress := CustomerAddress{
		Class:        addressClass,
		AddressLine1: "123 James Street",
		Suburb:       "Balcatta",
		City:         "Perth",
		State:        "WA",
		PostCode:     6000,
	}

	postalAddress := CustomerAddress{
		Class:        addressClass,
		AddressLine1: "PO Box 1000",
		AddressLine2: "Balcatta Post Office",
		Suburb:       "Balcatta",
		City:         "Perth",
		State:        "WA",
		PostCode:     6000,
	}

	customer := Customer{
		Class:              customerClass,
		ID:                 1,
		CustomerName:       "Tim",
		CustomerType:       "GOLD",
		HomeAddress:        homeAddress,
		PostalAddress:      postalAddress,
		OutstandingBalance: 10000,
	}

	_, err = namedMap.Put(ctx, customer.ID, customer)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	result, err = namedMap.Get(ctx, 1)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Not(gomega.BeNil()))
	g.Expect(customer.CustomerName).To(gomega.Equal("Tim"))
	g.Expect(customer.OutstandingBalance).To(gomega.Equal(float32(10000)))
	g.Expect(customer.HomeAddress).To(gomega.Equal(homeAddress))
	g.Expect(customer.HomeAddress.Class).To(gomega.Equal(addressClass))
	g.Expect(customer.PostalAddress).To(gomega.Equal(postalAddress))
	g.Expect(customer.PostalAddress.Class).To(gomega.Equal(addressClass))
	g.Expect(customer.Class).To(gomega.Equal(customerClass))

	// now do a test to ensure that the data exists on the server as a Java Object
	_, err = IssueGetRequest(GetTestContext().RestURL + "/checkCustomerCache/" + namedMap.Name())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}
