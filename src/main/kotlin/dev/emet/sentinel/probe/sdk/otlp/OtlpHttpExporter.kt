// OtlpHttpExporter posts ended spans and producer completeness to Sentinel.
//
// The body is uncompressed OTLP/HTTP protobuf. The source handle is the
// x-sentinel-source-handle header, not a resource attribute. Connect Decide
// is not this path.
package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
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

    // exportCompleteness posts one ProducerCompletenessSignal as an OTLP log.
    public fun exportCompleteness(signal: ProducerCompletenessSignal) {
        post(LOGS_PATH, OtlpCodec.encodeCompleteness(signal, sourceHandle))
    }

    private fun post(
        path: String,
        body: ByteArray,
    ) {
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
    }
}

// OtlpExportingSpanProcessor posts every ended span through OtlpHttpExporter.
// The host attaches it to the TracerProvider. The processor does not call Connect.
public class OtlpExportingSpanProcessor(
    private val exporter: OtlpHttpExporter,
) : SpanProcessor {
    override fun onStart(
        context: Context,
        parentSpan: ReadWriteSpan,
    ) {
        // Emission is an end-of-span concern. A span that has not ended is not evidence.
    }

    override fun isStartRequired(): Boolean = false

    override fun onEnd(span: ReadableSpan) {
        exporter.exportSpan(span.toSpanData())
    }

    override fun isEndRequired(): Boolean = true

    override fun shutdown(): CompletableResultCode = CompletableResultCode.ofSuccess()

    override fun forceFlush(): CompletableResultCode = CompletableResultCode.ofSuccess()
}
