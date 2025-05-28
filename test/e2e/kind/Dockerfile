# ----------------------------------------------------------------------------------------------------------------------
# Copyright (c) 2025, Oracle and/or its affiliates.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
# http://oss.oracle.com/licenses/upl.

FROM alpine AS builder

COPY runner /runner
RUN chmod 0555 /runner

FROM scratch

COPY --chown=1000:1000 --from=builder /runner /files/runner

USER 1000:1000

EXPOSE 8080

ENTRYPOINT ["/files/runner"]
CMD ["-h"]