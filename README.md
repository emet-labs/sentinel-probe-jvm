# JVM (Kotlin) Probe SDK for Sentinel

[![CI](https://github.com/emet-labs/sentinel-probe-jvm/actions/workflows/ci.yml/badge.svg)](https://github.com/emet-labs/sentinel-probe-jvm/actions/workflows/ci.yml)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL--2.0-informational.svg)](LICENSE)

The JVM Probe SDK for [Sentinel](https://github.com/emet-labs) (Kotlin, Java-interopable)
— instrument a JVM application as a Sentinel **Probe**: the in-process component that reports
what your application really did, as Events, and receives Sentinel's Verdicts and Event
Filters in return.

Sentinel verifies cross-system action sequences against Specifications you declare, using
recorded evidence rather than logs and dashboards. A Probe is how your code joins that
contract. Missing evidence is never a pass: until Sentinel can show it has seen everything
a source emitted, a Verdict that depends on that source stays Inconclusive.

## Requirements

- JDK 21 or later.
- A [Sentinel](https://github.com/emet-labs) deployment on the same host as the Probe.
  The receiver binds the IPv4 loopback address `127.0.0.1` by default. A Probe in a
  container passes a bind host and port (`OtlpLogReceiver("0.0.0.0", 4318)`) so Sentinel
  can reach it over the container network; the receiver has no authentication, so do that
  only on a network that only Sentinel can reach. Start it before Sentinel, following the
  order below.

## Installation

The library is not yet on Maven Central. Until it is, build it from this repository with
`./gradlew build` (the wrapper is committed), or add this repository as a Gradle
dependency. The generated protobuf types are committed, so a bare clone builds with no
code generator.

## Quickstart

The Probe wire is OTLP/HTTP protobuf, in both directions:

- each ended OTel span is exported as one Event to Sentinel's `POST /v1/traces`, with the
  `x-sentinel-source-handle` header naming your source;
- completeness signals go to `POST /v1/logs` on the same base URL, with the same
  `x-sentinel-source-handle` header;
- Sentinel posts Verdicts and published Event Filters as OTLP logs to the Probe's loopback
  log receiver.

Start the Probe and its log receiver first. Then start Sentinel with
`SENTINEL_PROBE_LOG_URL` set to the receiver's full `http://127.0.0.1:<port>/v1/logs`
URL, and publish the Specification only after the receiver is listening. Sentinel reads
this URL once at startup and does not retry a failed Filter push. If the receiver's port
changes when the Probe restarts, restart Sentinel with the new URL and publish again.
One Sentinel process has one Probe log URL, so only one Probe process receives these
Verdicts and Event Filters today.

Both ingest routes require a source handle admitted by your Sentinel deployment; replace
`checkout-service` below with that handle. A missing header returns HTTP 401; an unknown
handle returns HTTP 403, and neither stores evidence. A completeness record's
`sentinel.source_handle` must match the header; the SDK exporter sets both.

Sentinel accepts `Content-Type: application/x-protobuf`, with identity or gzip encoding.
OTLP/JSON, OTLP/gRPC and other encodings are unsupported (HTTP 415 on the HTTP route).
An invalid span can refuse the whole request during decoding, including an unknown
`sentinel.*` attribute. Admission conflicts may instead return HTTP 200 with nonzero
`partial_success.rejected_spans`; this is still a failed export for completeness.
The OTel span name becomes the Event kind.

```kotlin
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ProbeTracer
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.otlp.CompletenessHeartbeat
import dev.emet.sentinel.probe.sdk.otlp.OtlpExportingSpanProcessor
import dev.emet.sentinel.probe.sdk.otlp.OtlpHttpExporter
import dev.emet.sentinel.probe.sdk.otlp.OtlpLogReceiver
import io.opentelemetry.sdk.trace.SdkTracerProvider
import java.util.UUID

const val SENTINEL_URL = "http://127.0.0.1:8080"
const val SOURCE_HANDLE = "checkout-service"

fun main() {
    // The loopback log receiver: point Sentinel's SENTINEL_PROBE_LOG_URL at receiver.logUrl.
    val receiver = OtlpLogReceiver()

    // Holds the versioned Event Filter: each Event Filter the receiver accepts replaces it,
    // and the gate reads decisions from the receiver.
    val probe =
        ProbeClient.fromLogReceiver(
            ProbeClient.Config(sourceHandle = SOURCE_HANDLE, sentinelBaseUrl = SENTINEL_URL),
            receiver,
        )

    // One sequence allocator per source (see "Sequence numbers and completeness").
    val sequence = SequenceAllocator()

    // The host owns the TracerProvider. This processor exports each ended OTel span as one
    // Event to POST /v1/traces, and stamps sentinel.sequence.* when each span starts.
    val exporter = OtlpHttpExporter(SENTINEL_URL, SOURCE_HANDLE)
    val provider =
        SdkTracerProvider.builder()
            .addSpanProcessor(OtlpExportingSpanProcessor(exporter, sequence))
            .build()
    val tracer = ProbeTracer(provider, tracerName = SOURCE_HANDLE)

    // Declares this source's completeness on POST /v1/logs.
    val heartbeat = CompletenessHeartbeat(
        sequence = sequence,
        send = exporter::exportCompleteness,
        filterEpoch = probe::acknowledgedEpoch,
        onError = { it.printStackTrace() },
    )
    heartbeat.start()

    val span =
        tracer.tracer.spanBuilder("checkout.charge.completed")
            .setAttribute(ATTRIBUTE_EVENT_ID, UUID.randomUUID().toString())
            .setAttribute(ATTRIBUTE_SCHEMA_VERSION, "1.0.0")
            .setAttribute(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH, probe.acknowledgedEpoch() ?: 0L)
            .setAttribute(ATTRIBUTE_CLAIMED_SENSITIVITY, "SENSITIVITY_INTERNAL")
            .setAttribute("order.id", "order-1001")
            .startSpan()
    // ... the work the Specification watches ...
    span.end()

    // On a clean exit: flush the exports, then send the final completeness report.
    provider.shutdown()
    heartbeat.shutdown()
    receiver.close()
}
```

Every exported OTel span carries `sentinel.event.id`, `sentinel.schema.version`,
`sentinel.acknowledged_filter_epoch` (a `Long`, 0 until the first Event Filter arrives),
`sentinel.claimed_sensitivity` (a `Sensitivity` name other than `SENSITIVITY_UNSPECIFIED`)
and the two sequence attributes. A causal predecessor goes in `sentinel.parent.event.id`,
or in a link's own `sentinel.event.id`. An OTel span that breaks this contract is refused
with `SpanExportRefusal` before anything is sent, and an export Sentinel refuses is an
`OtlpTransportException` (with `statusCode` and the response body). Neither reaches your
code: the span-end path never throws. The error goes to `ExportOptions.onError` and a
rate-limited log line, and the span's Sequence slot is marked failed. A span id never
stands in for a missing Event id.

`ProbeClient.fromLogReceiver` passes each Event Filter the receiver accepts to
`setFilter`, which is a no-op when the epoch is unchanged or older. Pass
`ExportOptions(filter = { probe.currentFilter() })` and `OtlpExportingSpanProcessor` drops a
span no Specification selects and marks its slot filtered under the Filter's epoch (no Filter
held: every span is exported; attributes are not trimmed). `ApplyFilter.apply` projects a
`ProducerEvent` against the held Filter.

Start Probe spans as roots, or stamp the parent's Event id in
`sentinel.parent.event.id`: an ambient OTel parent without that Event id is refused.
Across a process boundary, carry the caller's current Event id in the
`x-sentinel-parent-event-id` header next to `traceparent` (HTTP and Kafka alike):
`injectParentEventId(headers, eventId)` on the caller, `extractParentEventId(headers)` on the
callee (it falls back to the `sentinel` member of `tracestate`); both also take a getter or
setter for any carrier. A caller that is not a Probe (a browser, curl, a harness) sends
neither header. Set `ExportOptions(allowUnattributedRemoteParent = true)` and a span whose
REMOTE parent carries no Event id is exported as a causal root, with no parent span id on
the copy Sentinel receives, so a Binding that needs that edge stays Inconclusive. Without
the option such a span is refused and holds `complete_through`. A local parent without the
Event id is refused either way.

### Export policy

`OtlpExportingSpanProcessor` exports asynchronously by default: `onEnd` only enqueues, and
one daemon worker posts. The queue is bounded (`queueCapacity`, 8192); when it is full the
span that just ended is dropped and its slot stays failed, so the epoch never closes over
the missing Event. Transport errors (no response, 408, 425, 429, 5xx) are retried with
exponential backoff, `maxAttempts` posts in all; a refusal, another 4xx, or a 2xx with
rejected spans is not retried. A span whose attempts ran out stays failed and is parked
(`parkedRetryCapacity`); while the queue is idle the worker re-posts the identical Event,
and `complete_through` catches up when Sentinel accepts it. `forceFlush()` and `shutdown()`
flush the queue, so shut the TracerProvider down before `heartbeat.shutdown()`.
`ExportOptions(synchronous = true)` posts inside `onEnd` instead, once.

### Enforcement gate

The gate is in the SDK, but Sentinel does not answer decision requests yet (see Status).

```kotlin
import dev.emet.sentinel.probe.sdk.SentinelProbeSdk
import dev.emet.sentinel.probe.sdk.enforcement.Options

// `event` is the ProducerEvent for the action, for example SentinelProbeSdk.spanToEvent on
// the SpanData a ProbeSpanProcessor callback receives. Ask before it becomes irreversible.
val outcome =
    receiver.enforce(
        event,
        probe.currentFilter(),
        null, // deadlineNs: an absolute monotonic deadline, if you set one
        System::nanoTime,
        ::acceptedFailMode, // the fail mode your deployment contracted per Specification
        Options(
            sourceHandle = SOURCE_HANDLE,
            requestID = SentinelProbeSdk.newRequestID(),
            idempotencyKey = SentinelProbeSdk.newIdempotencyKey(),
        ),
    )
if (outcome.permitted) commitTheAction() else rollback() // permitted: Permit, FailOpenPermit, NoFilter
```

## What the Probe does

| Package | Duty |
|---|---|
| `otlp` | The Probe wire. `OtlpHttpExporter` posts ended OTel spans to `POST /v1/traces` and completeness signals to `POST /v1/logs`; `OtlpExportingSpanProcessor` exports from a TracerProvider; `OtlpLogReceiver` is the log receiver (loopback by default) for Verdicts, Event Filters and DecideResponses; `CompletenessHeartbeat` reports completeness |
| `emission` | OTel spans become Events (`ProbeTracer`, `ProbeSpanProcessor`, `spanToEvent`). `SequenceAllocator` numbers the Events |
| `client` | Holds the versioned Event Filter for your source and reads decisions from the log receiver (`ProbeClient.fromLogReceiver`, `FilterStore`) |
| `filter` | Relevance projection — drops attributes no Specification needs. Never samples (`ApplyFilter`) |
| `enforcement` | The blocking decision — permit / deny / defer, with per-Specification fail modes and a monotonic latency budget (`gate`). Not answered yet: see Status |
| `config` | Source Tier resolution from deployment config, never hard-coded (`SourceTiers`) |
| `ids` | Per-call request and idempotency identifiers |

The `SentinelProbeSdk` facade object also exposes `applyFilter`, `gate`, `spanToEvent`,
`tierForHandle` and the identifier generators.

## Sequence numbers and completeness

To show it has seen everything a source emitted, Sentinel needs two things from the
source:

- **Sequence numbers.** Every exported Event carries `sentinel.sequence.epoch`, its
  Sequence Epoch, and `sentinel.sequence.number`: 1, 2, 3, … within that epoch.
- **Completeness signals.** A `ProducerCompletenessSignal` on `POST /v1/logs` declares, for
  the current epoch, `last_emitted` (the highest number allocated) and `complete_through`
  (the highest number up to which every Event was exported successfully), with the Filter
  epoch the Probe acknowledges. The source's Watermark advances from these signals.

The SDK provides both:

- `dev.emet.sentinel.probe.sdk.emission.SequenceAllocator`: create one per source and pass
  it to `OtlpExportingSpanProcessor(exporter, sequence)`. The processor then allocates a
  slot when each OTel span starts and stamps both sequence attributes. An OTel span that
  already carries both keeps them, so you can reserve a slot with `allocate()` ahead of
  its Event and stamp it on the span yourself.
- After each export the processor marks the slot. An export succeeded only when Sentinel
  answered HTTP 2xx with no rejected spans (`partial_success.rejected_spans` absent or 0;
  `OtlpHttpExporter.exportSpansChecked` returns that count); anything else marks the slot
  failed. If you allocate a slot and then drop its Event for the Event Filter, mark it
  with `markFiltered(slot, filterEpoch)`.
- `dev.emet.sentinel.probe.sdk.otlp.CompletenessHeartbeat`: after `start()` it reports the
  allocator's declaration on a `ScheduledExecutorService`, every 15 seconds by default;
  `report()` sends one on demand. It acknowledges the held Filter epoch (0 before the
  first Event Filter), sends one signal at a time, and restates the previous signal when
  nothing has changed. `nowUnixNanos` and `send` replace its clock and its transport.

**`complete_through` never runs ahead of the evidence.** It advances only across a
contiguous run of successful exports. A failed export, an OTel span that never ends, and
today an Event dropped for the Event Filter each hold it below that slot: the epoch stays
open and the Verdicts that depend on this source stay Inconclusive instead of passing.
The allocator itself does not retry; `OtlpExportingSpanProcessor` re-posts a failed span (see
"Export policy"). If you export by hand, re-export the identical Event and call
`markExported(slot)`.

**The epoch rolls after a declared close.** A signal whose `complete_through` equals
`last_emitted` is a declared close, and Sentinel accepts nothing more in that epoch. The
allocator seals it, and the next allocation opens a new epoch: the greater of the previous
epoch plus one and the clock's Unix milliseconds. Expect a new Sequence Epoch after every
quiet period. If Sentinel has not yet accepted the previous epoch's final declaration
under the Filter epoch the Probe now acknowledges, the heartbeat restates it before the
first signal of the new epoch.

**A restarted process needs a strictly greater epoch.** By default the first epoch is the
clock's Unix milliseconds. If your clock can step backwards across a restart, persist the
last epoch and pass it to the next run's `SequenceAllocator` as the previous epoch: the
allocator then starts at the greater of the clock and that epoch plus one, and refuses an
explicit epoch that is not greater. Without it, a clock that stepped backwards yields an
epoch that Sentinel refuses with HTTP 409.

**Idle sources.** `declareIdle()` reports the source as intentionally idle. The next
allocation, or `declareActive()`, makes it active again.

**`stop()` and `shutdown()`.** `stop()` cancels the schedule and sends nothing more: use it
when the process must go silent. `shutdown()` waits for a running report and sends one
final report, which is a declared close only if every slot was exported. On a clean exit,
shut the TracerProvider down first so its exports settle, then call `shutdown()`.

## Status

- **Detect-only.** Sentinel ingests Events, evaluates Specifications and reports Verdicts.
  It blocks nothing.
- **Decide is not served.** Sentinel answers no decision requests today. When an enforcing
  Specification selects an Event, `OtlpLogReceiver.decide` waits for a DecideResponse for
  at most the request's remaining Transport Budget. No answer arrives, so when the budget
  runs out the gate resolves by its fail mode: `FailClosedDeny` when one of those
  Specifications is fail-closed and your deployment accepted that, `FailOpenPermit`
  otherwise.
- **One wire.** OTLP/HTTP protobuf, as described above, is the only Probe wire.
- **Unversioned.** There is no release yet, and no compatibility guarantee before one: the
  API and the wire may still change. A `0.1.0` package manifest is not a release.

A Specification with no blocking budget or a zero budget resolves by fail mode at once.

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

The toolchain is tested with Devbox 0.19.0. Devbox release installs are a launcher that
runs `DEVBOX_USE_VERSION` when it is set, so `export DEVBOX_USE_VERSION=0.19.0` keeps you
on the tested version.
