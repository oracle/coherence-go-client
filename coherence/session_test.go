/*
* Copyright (c) 2022, 2023 Oracle and/or its affiliates.
* Licensed under the Universal Permissive License v 1.0 as shown at
* https://oss.oracle.com/licenses/upl.
 */

package coherence

import (
	"context"
	"github.com/onsi/gomega"
	"testing"
)

func TestSessionValidation(t *testing.T) {
	var (
		g   = gomega.NewWithT(t)
		err error
		ctx = context.Background()
	)

	_, err = NewSession(ctx, WithFormat("not-json"))
	g.Expect(err).To(gomega.Equal(ErrInvalidFormat))
}
