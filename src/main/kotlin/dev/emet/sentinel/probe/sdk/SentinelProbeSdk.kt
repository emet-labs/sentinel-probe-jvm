// SentinelProbeSdk is the public API entry point for the JVM Probe SDK. It is a thin facade
// that re-exports the per-package operations a Probe composes — filter projection, the
// enforcement gate, span-to-event conversion, identifier generation, source-tier resolution —
// so a host can wire the SDK with a single import.
//
// Each member delegates to the package that owns the logic; nothing is reimplemented here.
// The package-level functions in filter/, enforcement/, emission/, ids/ and config/ remain the
// canonical entry points and may be imported directly.
package dev.emet.sentinel.probe.sdk

import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SourceTier
import dev.emet.sentinel.probe.sdk.config.SourceTierMap
import dev.emet.sentinel.probe.sdk.config.SourceTiers
import dev.emet.sentinel.probe.sdk.emission.SpanConversion
import dev.emet.sentinel.probe.sdk.enforcement.Deps
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.sdk.enforcement.gate
import dev.emet.sentinel.probe.sdk.filter.ApplyFilter
import dev.emet.sentinel.probe.sdk.ids.ProbeIds

public object SentinelProbeSdk {
    // applyFilter projects event against filter (ADR-0006 relevance projection). Delegates to
    // filter.ApplyFilter; returns null when no Specification selects the event.
    public fun applyFilter(
        event: ProducerEvent?,
        filter: EventFilter?,
    ): ProducerEvent? = ApplyFilter.apply(event, filter)

    // gate enforces one ASK_AND_BLOCK event. Delegates to enforcement.gate.
    public fun gate(
        event: ProducerEvent,
        filter: EventFilter?,
        deadlineNs: Long?,
        deps: Deps,
        options: Options,
    ): GateOutcome = gate(event, filter, deadlineNs, deps, options)

    // spanToEvent converts an ended OTel span into a ProducerEvent. Delegates to
    // emission.spanToEvent.
    public fun spanToEvent(conversion: SpanConversion): ProducerEvent =
        dev.emet.sentinel.probe.sdk.emission
            .spanToEvent(conversion)

    // newRequestID and newIdempotencyKey generate the per-call UUIDv4 identifiers a Probe
    // stamps into decision requests. Delegates to ids.ProbeIds.
    public fun newRequestID(): String = ProbeIds.generateRequestID()

    public fun newIdempotencyKey(): String = ProbeIds.generateIdempotencyKey()

    // tierForHandle resolves the proto SourceTier for a source_handle. Delegates to
    // config.SourceTiers; throws IllegalArgumentException for an undeclared or unknown tier.
    public fun tierForHandle(
        config: SourceTierMap,
        sourceHandle: String,
    ): SourceTier = SourceTiers.tierForHandle(config, sourceHandle)
}
