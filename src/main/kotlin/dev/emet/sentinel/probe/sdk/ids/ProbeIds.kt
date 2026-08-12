// Package ids generates the per-call identifiers a Probe stamps into decision requests.
// Kotlin analog of sdk/go/ids, which uses github.com/google/uuid. Here the JDK's UUID is the
// zero-dependency equivalent: java.util.UUID.randomUUID() is a v4 UUID, exactly what the Go
// and TypeScript references produce.
package dev.emet.sentinel.probe.sdk.ids

import java.util.UUID

public object ProbeIds {
    // GenerateRequestID returns a fresh request ID (UUID v4).
    public fun generateRequestID(): String = UUID.randomUUID().toString()

    // GenerateIdempotencyKey returns a fresh idempotency key (UUID v4).
    //
    // A distinct function from generateRequestID even though the implementation is identical:
    // the two identify different things. A retry of the same logical decision reuses the
    // idempotency key while taking a new request ID, so collapsing them would make retries
    // indistinguishable from fresh asks at the receiver.
    public fun generateIdempotencyKey(): String = UUID.randomUUID().toString()
}
