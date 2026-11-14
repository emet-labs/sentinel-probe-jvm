// OtlpHttpExporter posts ended spans and producer completeness to Sentinel.
//
// The body is uncompressed OTLP/HTTP protobuf. The source handle is the
// x-sentinel-source-handle header, not a resource attribute. Connect Decide
// is not this path.
package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.Slot
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.context.Context
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.ReadWriteSpan
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.SpanProcessor
import io.opentelemetry.sdk.trace.data.SpanData
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

// OtlpTransportException is an HTTP failure. The span was not accepted.
public class OtlpTransportException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

public class OtlpHttpExporter(
    baseUrl: String,
    private val sourceHandle: String,
    private val httpClient: HttpClient =
        HttpClient
            .newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build(),
) {
    private val origin: String = baseUrl.trimEnd('/')

    init {
        if (sourceHandle.isBlank()) throw SpanExportRefusal("source handle is blank")
    }

    // exportSpan posts one ended span. The span must already carry the #398 attributes.
    // A missing predecessor id fails the call and is not sent as a span id.
    public fun exportSpan(span: SpanData) {
        exportSpans(listOf(span))
    }

    public fun exportSpans(spans: List<SpanData>) {
        post(TRACES_PATH, OtlpCodec.encodeTrace(spans))
    }

    // exportSpansChecked posts the spans and returns the rejected span count from Sentinel's
    // ExportTraceServiceResponse. The export succeeded only when it returns 0. A non-2xx status
    // or an undecodable 2xx body throws OtlpTransportException.
    public fun exportSpansChecked(spans: List<SpanData>): Long {
        val body = post(TRACES_PATH, OtlpCodec.encodeTrace(spans))
        return try {
            PartialSuccess.rejectedSpans(body)
        } catch (error: IllegalArgumentException) {
            throw OtlpTransportException("OTLP POST $TRACES_PATH returned an undecodable response: ${error.message}", error)
        }
    }

    // exportCompleteness posts one ProducerCompletenessSignal as an OTLP log.
    public fun exportCompleteness(signal: ProducerCompletenessSignal) {
        post(LOGS_PATH, OtlpCodec.encodeCompleteness(signal, sourceHandle))
    }

    private fun post(
        path: String,
        body: ByteArray,
    ): ByteArray {
        val request =
            HttpRequest
                .newBuilder()
                .uri(URI.create(origin + path))
                .header("Content-Type", PROTOBUF_MEDIA_TYPE)
                .header(SOURCE_HANDLE_HEADER, sourceHandle)
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build()
        val response =
            try {
                httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray())
            } catch (error: IOException) {
                throw OtlpTransportException("OTLP POST $path failed: ${error.message}", error)
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                throw OtlpTransportException("OTLP POST $path interrupted", error)
            }
        if (response.statusCode() !in 200..299) {
            val text =
                response
                    .body()
                    ?.toString(Charsets.UTF_8)
                    ?.take(500)
                    .orEmpty()
            throw OtlpTransportException("OTLP POST $path returned HTTP ${response.statusCode()}: $text")
        }
        return response.body() ?: ByteArray(0)
    }
}

// OtlpExportingSpanProcessor posts every ended span through OtlpHttpExporter.
// The host attaches it to the TracerProvider. The processor does not call Connect.
//
// With a SequenceAllocator, the processor stamps sentinel.sequence.epoch and
// sentinel.sequence.number when a span starts, before any Event Filter, unless the host already
// set both (a slot reserved ahead of its Event). When the span ends it marks the slot exported
// only on HTTP 2xx with zero rejected spans, and failed otherwise; a failure is rethrown as
// before. Without an allocator the processor behaves as it always has.
public class OtlpExportingSpanProcessor(
    private val exporter: OtlpHttpExporter,
    private val sequence: SequenceAllocator? = null,
) : SpanProcessor {
    override fun onStart(
        context: Context,
        parentSpan: ReadWriteSpan,
    ) {
        // Without an allocator, emission is an end-of-span concern.
        val allocator = sequence ?: return
        if (parentSpan.getAttribute(SEQUENCE_EPOCH_KEY) != null && parentSpan.getAttribute(SEQUENCE_NUMBER_KEY) != null) return
        val slot = allocator.allocate()
        parentSpan.setAttribute(SEQUENCE_EPOCH_KEY, slot.epoch)
        parentSpan.setAttribute(SEQUENCE_NUMBER_KEY, slot.number)
    }

    override fun isStartRequired(): Boolean = sequence != null

    @Suppress("TooGenericExceptionCaught")
    override fun onEnd(span: ReadableSpan) {
        val allocator = sequence
        if (allocator == null) {
            exporter.exportSpan(span.toSpanData())
            return
        }
        val epoch = span.getAttribute(SEQUENCE_EPOCH_KEY)
        val number = span.getAttribute(SEQUENCE_NUMBER_KEY)
        val slot = if (epoch != null && number != null) Slot(epoch, number) else null
        val rejected =
            try {
                exporter.exportSpansChecked(listOf(span.toSpanData()))
            } catch (error: RuntimeException) {
                // A client-side refusal, an HTTP failure or an undecodable response: Sentinel
                // does not hold the Event, so the slot must not count as exported.
                slot?.let(allocator::markFailed)
                throw error
            }
        if (slot == null) return
        if (rejected == 0L) allocator.markExported(slot) else allocator.markFailed(slot)
    }

    override fun isEndRequired(): Boolean = true

    override fun shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

    override fun forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()

    private companion object {
        val SEQUENCE_EPOCH_KEY: AttributeKey<Long> = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH)
        val SEQUENCE_NUMBER_KEY: AttributeKey<Long> = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER)
    }
}
