package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.proto.collector.trace.v1.ExportTracePartialSuccess
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.sdk.trace.data.StatusData
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.HexFormat
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PartialSuccessTests {
    private fun hex(value: String): ByteArray = HexFormat.of().parseHex(value)

    @Test
    fun `an empty body rejects nothing`() {
        assertEquals(0L, PartialSuccess.rejectedSpans(ByteArray(0)))
    }

    @Test
    fun `rejected spans are read from partial_success`() {
        assertEquals(2L, PartialSuccess.rejectedSpans(hex("0a020802")))
    }

    @Test
    fun `a duplicate message with zero rejected spans rejects nothing`() {
        val body =
            ExportTraceServiceResponse
                .newBuilder()
                .setPartialSuccess(
                    ExportTracePartialSuccess.newBuilder().setErrorMessage("append_events classified 1 deliveries as duplicate"),
                ).build()
                .toByteArray()
        assertEquals(0L, PartialSuccess.rejectedSpans(body))
    }

    @Test
    fun `a truncated body is undecodable`() {
        assertFailsWith<IllegalArgumentException> { PartialSuccess.rejectedSpans(hex("0a0508")) }
    }

    @Test
    fun `a negative rejected count is undecodable`() {
        val body =
            ExportTraceServiceResponse
                .newBuilder()
                .setPartialSuccess(ExportTracePartialSuccess.newBuilder().setRejectedSpans(-1L))
                .build()
                .toByteArray()
        assertFailsWith<IllegalArgumentException> { PartialSuccess.rejectedSpans(body) }
    }

    @Test
    fun `exportSpansChecked returns the rejected count from a 200`() {
        withSentinel(200, hex("0a020801")) { origin ->
            assertEquals(1L, OtlpHttpExporter(origin, "sagashop-payment").exportSpansChecked(listOf(span())))
        }
        withSentinel(200, ByteArray(0)) { origin ->
            assertEquals(0L, OtlpHttpExporter(origin, "sagashop-payment").exportSpansChecked(listOf(span())))
        }
    }

    @Test
    fun `exportSpansChecked fails on a non-2xx status or an undecodable 200`() {
        withSentinel(500, "boom".toByteArray()) { origin ->
            assertFailsWith<OtlpTransportException> { OtlpHttpExporter(origin, "sagashop-payment").exportSpansChecked(listOf(span())) }
        }
        withSentinel(200, hex("0a0508")) { origin ->
            assertFailsWith<OtlpTransportException> { OtlpHttpExporter(origin, "sagashop-payment").exportSpansChecked(listOf(span())) }
        }
    }

    private fun withSentinel(
        status: Int,
        body: ByteArray,
        block: (String) -> Unit,
    ) {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            exchange.requestBody.readBytes()
            exchange.sendResponseHeaders(status, if (body.isEmpty()) -1 else body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
        server.start()
        try {
            block("http://127.0.0.1:${server.address.port}")
        } finally {
            server.stop(0)
        }
    }

    private fun span() =
        TestSpanData
            .builder()
            .setName("sagashop.payment.charge.completed")
            .setKind(SpanKind.INTERNAL)
            .setSpanContext(
                SpanContext.create(
                    "0102030405060708090a0b0c0d0e0f10",
                    "3132333435363738",
                    TraceFlags.getDefault(),
                    TraceState.getDefault(),
                ),
            ).setParentSpanContext(SpanContext.getInvalid())
            .setStartEpochNanos(1_700_000_000_000_000_000L)
            .setEndEpochNanos(1_700_000_000_005_000_000L)
            .setAttributes(
                Attributes
                    .builder()
                    .put(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), "01f0c0ffee-charge")
                    .put(AttributeKey.stringKey(ATTRIBUTE_SCHEMA_VERSION), "sagashop-contract-v1")
                    .put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH), 7L)
                    .put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER), 1L)
                    .put(AttributeKey.longKey(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH), 4L)
                    .put(AttributeKey.stringKey(ATTRIBUTE_CLAIMED_SENSITIVITY), Sensitivity.SENSITIVITY_INTERNAL.name)
                    .build(),
            ).setStatus(StatusData.unset())
            .setHasEnded(true)
            .setTotalRecordedEvents(0)
            .setTotalRecordedLinks(0)
            .setTotalAttributeCount(6)
            .setResource(Resource.getDefault())
            .setInstrumentationScopeInfo(InstrumentationScopeInfo.empty())
            .build()
}
