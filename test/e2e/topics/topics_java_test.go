/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl.
 */

package topics

import (
	"context"
	"fmt"
	"github.com/onsi/gomega"
	"github.com/oracle/coherence-go-client/v2/coherence"
	"github.com/oracle/coherence-go-client/v2/coherence/publisher"
	"github.com/oracle/coherence-go-client/v2/coherence/subscriber"
	"github.com/oracle/coherence-go-client/v2/test/utils"
	"strconv"
	"testing"
	"time"
)

// these must be defined in META-INF/type-aliases.properties on the server like:
// test.customer=com.oracle.coherence.go.testing.Customer
// test.address=com.oracle.coherence.go.testing.Address

const (
	customerClass = "test.customer"
	addressClass  = "test.address"
)

func TestTopicsJavaObjectsCreatedOnServer(t *testing.T) {
	RunTestTopicsJavaObjectsOnServer(gomega.NewWithT(t), true)
}

func TestTopicsJavaObjectsCreatedOnClient(t *testing.T) {
	RunTestTopicsJavaObjectsOnServer(gomega.NewWithT(t), false)
}

func RunTestTopicsJavaObjectsOnServer(g *gomega.GomegaWithT, createOnServer bool) {
	var (
		err          error
		ctx          = context.Background()
		sub1         coherence.Subscriber[utils.Customer]
		timeout      = time.Second * 200
		messageCount counter
		topicName    = fmt.Sprintf("my-topic-java-server-%v", createOnServer)
		maxMessages  = 5_000
		pub1         coherence.Publisher[utils.Customer]
	)

	session1, topic1 := getSessionAndTopic[utils.Customer](g, topicName)
	defer session1.Close()

	sub1, err = topic1.CreateSubscriber(ctx, subscriber.InSubscriberGroup("group1"))
	g.Expect(err).NotTo(gomega.HaveOccurred())

	if createOnServer {
		// create the topic on the server and publish maxMessages messages
		_, err = utils.IssueGetRequest(utils.GetTestContext().RestURL + "/createCustomerTopic/" + topicName + "/" + fmt.Sprintf("%d", maxMessages))
		g.Expect(err).Should(gomega.Not(gomega.HaveOccurred()))
	} else {
		pub1, err = topic1.CreatePublisher(context.Background(), publisher.WithDefaultOrdering())
		g.Expect(err).ShouldNot(gomega.HaveOccurred())

		// publish the maxMessages messages
		for i := 0; i < maxMessages; i++ {
			c := utils.Customer{
				CustomerName:       fmt.Sprintf("name-%d", i),
				CustomerType:       "GOLD",
				ID:                 i,
				Class:              customerClass,
				OutstandingBalance: float32(i) * 1000,
				HomeAddress:        getAddress(i),
				PostalAddress:      getAddress(i),
			}

			_, err1 := pub1.Publish(ctx, c)
			g.Expect(err1).Should(gomega.Not(gomega.HaveOccurred()))
		}
	}

	// attempt to read the messages
	go func() {
		var (
			err1       error
			response   []*subscriber.ReceiveResponse[utils.Customer]
			start      = time.Now()
			subTimeout = timeout - (time.Second * 5)
		)

		for count := 0; count < maxMessages; count++ {
			response, err1 = sub1.Receive(context.Background())
			g.Expect(err1).ShouldNot(gomega.HaveOccurred())

			if len(response) == 0 {
				if time.Since(start) > subTimeout {
					g.Fail("timeout, receiving messages")
				}
				time.Sleep(250 * time.Millisecond)
				continue
			}

			g.Expect(len(response)).To(gomega.Equal(1))
			c := response[0].Value
			g.Expect(c.ID).To(gomega.Equal(count), fmt.Sprintf("expected ID to equal %d, but got %d", count, c.ID))
			g.Expect(c.Class).To(gomega.Equal(customerClass))
			g.Expect(c.CustomerName).To(gomega.Equal(fmt.Sprintf("name-%d", count)))
			g.Expect(c.OutstandingBalance).To(gomega.Equal(float32(1000) * float32(count)))
			g.Expect(c.CustomerType).To(gomega.Equal("GOLD"))

			g.Expect(isValidAddress(c.PostalAddress, count)).To(gomega.BeTrue())
			g.Expect(isValidAddress(c.HomeAddress, count)).To(gomega.BeTrue())
			_, err = sub1.Commit(ctx, response[0].Channel, response[0].Position)
			g.Expect(err).ShouldNot(gomega.HaveOccurred())
			messageCount.Increment()
		}
	}()

	g.Eventually(func() int { return messageCount.Get() }, timeout+(time.Second*10)).Should(gomega.Equal(maxMessages))

	g.Expect(sub1.Close(ctx)).ShouldNot(gomega.HaveOccurred())

	err = topic1.Destroy(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
}

func isValidAddress(address utils.CustomerAddress, counter int) bool {
	if address.AddressLine1 != fmt.Sprintf("address-line-1-%d", counter) {
		return false
	}
	if address.AddressLine2 != fmt.Sprintf("address-line-2-%d", counter) {
		return false
	}
	if address.City != fmt.Sprintf("City-%d", counter) {
		return false
	}
	if address.State != fmt.Sprintf("State-%d", counter) {
		return false
	}
	if address.Suburb != fmt.Sprintf("Suburb-%d", counter) {
		return false
	}
	if address.PostCode != counter {
		return false
	}
	if address.Class != addressClass {
		return false
	}
	return true
}

func getAddress(id int) utils.CustomerAddress {
	suffix := strconv.Itoa(id)
	return utils.CustomerAddress{
		Class:        addressClass,
		AddressLine1: "address-line-1-" + suffix,
		AddressLine2: "address-line-2-" + suffix,
		Suburb:       "Suburb-" + suffix,
		City:         "City-" + suffix,
		State:        "State-" + suffix,
		PostCode:     id,
	}
}
