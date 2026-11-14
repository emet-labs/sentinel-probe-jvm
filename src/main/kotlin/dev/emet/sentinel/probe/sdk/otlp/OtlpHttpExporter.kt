// OtlpHttpExporter posts ended spans and producer completeness to Sentinel.
//
// The body is uncompressed OTLP/HTTP protobuf. The source handle is the
// x-sentinel-source-handle header, not a resource attribute. Connect Decide
// is not this path.
package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import io.opentelemetry.sdk.trace.data.SpanData
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

// OtlpTransportException is an HTTP failure. The span was not accepted. statusCode is the HTTP
// status, null when no response arrived (connect failure, timeout, interrupt). responseBody is
// the start of the response body (at most 500 characters), empty when there is none; it is also
// in the message. retryAfterMillis is the server's Retry-After on a 429 or 503 (delta-seconds
// or an HTTP-date), null when absent, unparseable or on any other status.
public class OtlpTransportException(
    message: String,
    cause: Throwable? = null,
    public val statusCode: Int? = null,
    public val responseBody: String = "",
    public val retryAfterMillis: Long? = null,
) : RuntimeException(message, cause) {
    // isRetryable is the Probe SDK retry policy (sdk/README.md): true for no response and for
    // HTTP 408, 425, 429, 500, 502, 503 and 504. Any other status (400, 409, 501, ...) is a
    // verdict on the payload; repeating it changes nothing.
    public val isRetryable: Boolean
        get() = statusCode == null || statusCode in RETRYABLE_STATUSES

    internal companion object {
        val RETRYABLE_STATUSES: Set<Int> = setOf(408, 425, 429, 500, 502, 503, 504)

        // parseRetryAfter reads a Retry-After header, honored on 429 and 503 only.
        fun parseRetryAfter(
            status: Int,
            header: String?,
            now: java.time.Instant = java.time.Instant.now(),
        ): Long? {
            if (header == null || (status != 429 && status != 503)) return null
            val text = header.trim()
            if (text.isNotEmpty() && text.all { it in '0'..'9' }) {
                return text.toLongOrNull()?.coerceAtMost(Long.MAX_VALUE / 1000)?.times(1000)
            }
            return try {
                val at = java.time.ZonedDateTime.parse(text, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
                Duration.between(now, at.toInstant()).toMillis().coerceAtLeast(0)
            } catch (ignored: java.time.format.DateTimeParseException) {
                null
            }
        }
    }
}

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

    private companion object {
        const val MAX_ERROR_BODY_CHARS = 500
    }

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
    public fun exportSpansChecked(spans: List<SpanData>): Long = postEncodedTrace(OtlpCodec.encodeTrace(spans))

    // postEncodedTrace posts a body OtlpCodec.encodeTrace built and returns the rejected span
    // count, so a retry re-sends the identical bytes.
    internal fun postEncodedTrace(encoded: ByteArray): Long = postEncodedTraceOutcome(encoded).rejectedSpans

    // postEncodedTraceOutcome is postEncodedTrace that keeps Sentinel's error_message too.
    internal fun postEncodedTraceOutcome(encoded: ByteArray): Rejection {
        val body = post(TRACES_PATH, encoded)
        return try {
            PartialSuccess.rejection(body)
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
                    ?.take(MAX_ERROR_BODY_CHARS)
                    .orEmpty()
            val shown = text.ifBlank { "(empty body)" }
            throw OtlpTransportException(
                "OTLP POST $path returned HTTP ${response.statusCode()}: $shown",
                statusCode = response.statusCode(),
                responseBody = text,
                retryAfterMillis =
                    OtlpTransportException.parseRetryAfter(
                        response.statusCode(),
                        response.headers().firstValue("Retry-After").orElse(null),
                    ),
            )
        }
        return response.body() ?: ByteArray(0)
    }
}
