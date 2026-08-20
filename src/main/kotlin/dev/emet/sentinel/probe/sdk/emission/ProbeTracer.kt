// ProbeTracer bundles the tracer a Probe emits with, its provider, and the conversion into
// ProducerEvents. Kotlin analog of sdk/go/emission/tracer.go.
//
// Divergence from the Go reference: this ProbeTracer accepts an EXISTING TracerProvider and
// never starts its own, per the plan's ADR-0002 reading — the host owns export, so the host
// builds the TracerProvider (with whichever span processors, samplers and resource it wants)
// and hands it to the SDK. The SDK never decides where spans go. The Go reference builds a
// provider for convenience; here that convenience would hide export wiring from the host.
package dev.emet.sentinel.probe.sdk.emission

import dev.emet.sentinel.model.v1.ProducerEvent
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.trace.SdkTracerProvider

// ProbeTracer wraps an existing TracerProvider and the Tracer for a Probe's instrumentation
// scope. It never creates the provider: the caller owns its lifecycle (including shutdown).
public class ProbeTracer(
    // provider is the host-owned TracerProvider. The SDK does not shut it down.
    public val provider: SdkTracerProvider,
    // tracerName identifies the instrumentation scope, for example the Probe's package path.
    private val tracerName: String,
) {
    // tracer is the OTel Tracer the Probe emits spans with.
    public val tracer: Tracer = provider.get(tracerName)

    // toEvent converts an ended span into a ProducerEvent. Convenience wrapper over spanToEvent,
    // mirroring the reference's ProbeTracer.toEvent.
    public fun toEvent(conversion: SpanConversion): ProducerEvent = spanToEvent(conversion)

    // newProbeTracer builds a ProbeTracer over an existing SdkTracerProvider. The host is
    // expected to attach a ProbeSpanProcessor (and its exporter) to the provider's builder
    // before this call; the SDK never attaches processors itself.
    public companion object {
        @JvmStatic
        public fun newProbeTracer(
            provider: SdkTracerProvider,
            tracerName: String,
        ): ProbeTracer = ProbeTracer(provider, tracerName)
    }
}
