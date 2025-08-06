/*
 * Copyright (c) 2023, 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package standalone

import (
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"testing"
)

// TestBasicOperationsAgainstMapAndCache runs all tests against NamedMap and NamedCache
func TestJavaSerializationAgainstMapAndCache(t *testing.T) {
	g := gomega.NewWithT(t)
	session, err := utils.GetSession()
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer session.Close()

	testCases := []struct {
		testName string
		nameMap  coherence.NamedMap[int, utils.Customer]
		test     func(t *testing.T, namedCache coherence.NamedMap[int, utils.Customer])
	}{
		{"NamedMapSerializationTest", GetNamedMap[int, utils.Customer](g, session, "customer-map"), RunSerializationTest},
		{"NamedCacheSerializationTest", GetNamedCache[int, utils.Customer](g, session, "customer-cache"), RunSerializationTest},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc.test(t, tc.nameMap)
		})
	}
}

func RunSerializationTest(t *testing.T, namedMap coherence.NamedMap[int, utils.Customer]) {
	var (
		g      = gomega.NewWithT(t)
		result *utils.Customer
		err    error
	)

	// these must be defined in META-INF/type-aliases.properties on the server like:
	// test.customer=com.oracle.coherence.go.testing.Customer
	// test.address=com.oracle.coherence.go.testing.Address
	const customerClass = "test.customer"
	const addressClass = "test.address"

	err = namedMap.Clear(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	homeAddress := utils.CustomerAddress{
		Class:        addressClass,
		AddressLine1: "123 James Street",
		Suburb:       "Balcatta",
		City:         "Perth",
		State:        "WA",
		PostCode:     6000,
	}

	postalAddress := utils.CustomerAddress{
		Class:        addressClass,
		AddressLine1: "PO Box 1000",
		AddressLine2: "Balcatta Post Office",
		Suburb:       "Balcatta",
		City:         "Perth",
		State:        "WA",
		PostCode:     6000,
	}

	customer := utils.Customer{
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
	_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/checkCustomerCache/" + namedMap.Name())
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}
