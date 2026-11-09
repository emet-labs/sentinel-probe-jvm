// ProbeTracer bundles the tracer a Probe emits with, its provider, and the conversion into
// ProducerEvents. Kotlin analog of sdk/go/emission/tracer.go.
//
// The host still builds the TracerProvider. The Probe wire is the ADR-0002 addendum: ended
// spans leave through OtlpHttpExporter (OTLP/HTTP protobuf), which the host points at the
// Sentinel base URL. This type does not start that exporter and does not shut the provider
// down. The Go reference builds a provider for convenience; hiding the base URL inside this
// constructor would hide the wire from the host.
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

    // newProbeTracer builds a ProbeTracer over an existing SdkTracerProvider. The host
    // attaches OtlpExportingSpanProcessor, or calls OtlpHttpExporter itself, before spans
    // end. This constructor does not attach a processor.
    public companion object {
        @JvmStatic
        public fun newProbeTracer(
            provider: SdkTracerProvider,
            tracerName: String,
        ): ProbeTracer = ProbeTracer(provider, tracerName)
    }
}
