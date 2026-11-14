// ProbeSpanProcessor is the SpanProcessor.onEnd hook that captures ended spans and converts
// them to ProducerEvents. Kotlin analog of the emission hook the Go reference leaves to the
// host; here it is provided so a Probe can wire emission into a TracerProvider without writing
// OTel plumbing.
//
// It is a NO-OP processor on start (isStartRequired = false) and acts on end
// (isEndRequired = true): on every ended span it calls the host-provided callback with the
// span's SpanData (via ReadableSpan.toSpanData()), so the host receives typed attributes and
// causal edges to convert, project and enforce.
package dev.emet.sentinel.probe.sdk.emission

import io.opentelemetry.context.Context
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.ReadWriteSpan
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.SpanProcessor
import io.opentelemetry.sdk.trace.data.SpanData

// ProbeSpanProcessor forwards every ended span's SpanData to a host callback. The callback is
// the emission seam: a Probe implements it to run spanToEvent, or attaches
// OtlpExportingSpanProcessor to post the ended span on the OTLP wire.
public class ProbeSpanProcessor(
    // onEnd is invoked synchronously on span end with the span's SpanData. It must not block on
    // network: the enforcement gate's injected decide dependency is where blocking work belongs.
    private val onEnd: (span: SpanData) -> Unit,
    // onError receives anything the callback throws. The span-end path runs inside host code and
    // never throws: a failing callback is caught here.
    private val onError: (Throwable) -> Unit = {},
) : SpanProcessor {
    override fun onStart(
        context: Context,
        parentSpan: ReadWriteSpan,
    ) {
        // No-op: emission is an end-of-span concern.
    }

    override fun isStartRequired(): Boolean = false

    @Suppress("TooGenericExceptionCaught")
    override fun onEnd(span: ReadableSpan) {
        try {
            // toSpanData() captures the span's typed attributes, links and timing as a stable
            // snapshot, so the callback runs against ended data even if the SDK recycles the span.
            onEnd(span.toSpanData())
        } catch (error: Throwable) {
            try {
                onError(error)
            } catch (ignored: Throwable) {
                // A failing error callback must not reach the host either.
            }
        }
    }

    override fun isEndRequired(): Boolean = true

    override fun shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

    override fun forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()
}
