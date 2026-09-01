# JVM (Kotlin) Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-jvm/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-jvm/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The JVM Probe SDK for [Sentinel](https://github.com/emet-labs) (Kotlin, Java-interopable)
— instrument a JVM service as a Sentinel **Probe**: the in-process component that reports
what your service really did, and asks Sentinel whether to proceed before an action
becomes irreversible.

Sentinel verifies cross-system action sequences against rules you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract.

## Requirements

- JDK 21 or later.
- A running [Sentinel](https://github.com/emet-labs) deployment to connect to.

## Installation

The library is not yet on Maven Central. Until it is, build it from this repository with
`./gradlew build` (the wrapper is committed), or add this repository as a Gradle
dependency. The generated protobuf types are committed, so a bare clone builds with no
code generator.

## Quickstart

```kotlin
import dev.emet.sentinel.probe.sdk.SentinelProbeSdk
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.emission.ProbeTracer
import io.opentelemetry.sdk.trace.SdkTracerProvider

fun checkoutCharge() {
    // Connect to Sentinel's decision endpoint and identify this Probe.
    // The decider carries requests to Sentinel — see the client package.
    val probe = ProbeClient(
        ProbeClient.Config(sourceHandle = "checkout-service", sentinelBaseUrl = sentinelUrl),
        decider,
    )

    // The tracer your service emits evidence with. The host owns the
    // TracerProvider (and its exporter); the SDK never decides where spans go.
    val provider: SdkTracerProvider = buildHostTracerProvider()
    val tracer = ProbeTracer(provider, tracerName = "checkout-service")

    val span = tracer.tracer.spanBuilder("charge.card").startSpan()
    // ... the work the Specification watches ...
    span.end()

    // Convert the ended span into the event Sentinel reasons about.
    val event = SentinelProbeSdk.spanToEvent(conversion)

    // Ask before the action becomes irreversible.
    val outcome = SentinelProbeSdk.gate(
        event,
        probe.currentFilter(),
        null,  // deadlineNs: a monotonic deadline, if you set one
        Deps(
            decide = probe.decider(),
            nowMonotonicNs = ::monotonicNanos,
            acceptedFailModeFor = ::acceptedFailMode,  // what you contracted to
        ),
        Options(
            sourceHandle = probe.sourceHandle(),
            requestID = SentinelProbeSdk.newRequestID(),
            idempotencyKey = SentinelProbeSdk.newIdempotencyKey(),
        ),
    )

    when (outcome.kind) {
        PERMIT, FAIL_OPEN_PERMIT, NO_FILTER -> commitTheAction()
        // No filter held means the conservative default: proceed, fail-open.
        else -> rollback() // DENY | DEFER | FAIL_CLOSED: block, or retry within the budget
    }
}
```

When Sentinel publishes a new Event Filter epoch for this source, swap it in:

```kotlin
probe.setFilter(newFilter) // a no-op when the epoch is unchanged
```

## What the Probe does

| Package | Duty |
|---|---|
| `client` | Holds the versioned Event Filter for your source; builds decision requests (`ProbeClient`, `FilterStore`) |
| `filter` | Relevance projection before shipping — drops attributes no Specification needs. Never samples (`ApplyFilter`) |
| `emission` | OTel spans become Sentinel events (`ProbeTracer`, `spanToEvent`) |
| `enforcement` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`gate`) |
| `config` | Source-tier resolution from deployment config, never hard-coded (`SourceTiers`) |
| `ids` | Per-call request and idempotency identifiers |

Everything is also reachable through the `SentinelProbeSdk` facade object.

## Status

Early, pre-1.0. The wire protocol is versioned per release, but the API surface may
still change.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). This repository is a published mirror; pull
requests are reviewed and re-landed upstream.

## License

[MPL-2.0](LICENSE)

## Development environment (this mirror): Devbox + just

This repository pins its own toolchain — `devbox.json` + `devbox.lock` — and every task
runs inside it, the same convention as the Sentinel source repository:

    devbox install        # once; resolves devbox.lock
    devbox shell          # then `just --list` for the recipes

`build`, `test`, `lint` and `fmt-check` reuse the canonical gate names of the source
repository, scoped to this one language.
