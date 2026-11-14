package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.sdk.emission.FilteredRange
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.Slot
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.HexFormat
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OtlpExportingSpanProcessorTests {
    private val epochKey = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH)
    private val numberKey = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER)
    private val clock = AtomicLong(1_794_000_000_000L * 1_000_000L)
    private val sequence = SequenceAllocator(nowUnixNanos = clock::get)

    @Test
    fun `a span is stamped at start and an accepted export marks it exported`() {
        withSentinel(200, ByteArray(0)) { sentinel ->
            val span = start(sentinel)
            val stamped = Slot((span as ReadableSpan).getAttribute(epochKey)!!, span.getAttribute(numberKey)!!)
            assertEquals(Slot(sequence.epoch(), 1L), stamped)
            assertEquals(0L, sequence.declaration().completeThrough, "a started span is in flight")
            span.end()
            assertEquals(1L, sequence.declaration().completeThrough)
            val wire =
                ExportTraceServiceRequest
                    .parseFrom(sentinel.bodies.single())
                    .resourceSpansList
                    .single()
                    .scopeSpansList
                    .single()
                    .spansList
                    .single()
            val attributes = wire.attributesList.associate { it.key to it.value }
            assertEquals(stamped.epoch, attributes.getValue(ATTRIBUTE_SEQUENCE_EPOCH).intValue)
            assertEquals(1L, attributes.getValue(ATTRIBUTE_SEQUENCE_NUMBER).intValue)
        }
    }

    @Test
    fun `a 200 with a rejected span marks the slot failed`() {
        withSentinel(200, HexFormat.of().parseHex("0a020801")) { sentinel ->
            val span = start(sentinel)
            span.end()
            assertFailed(Slot(sequence.epoch(), 1L))
        }
    }

    @Test
    fun `an HTTP failure marks the slot failed and does not throw out of end`() {
        val errors = ArrayList<Throwable>()
        withSentinel(500, "unavailable".toByteArray()) { sentinel ->
            val span = start(sentinel, options = ExportOptions(synchronous = true, onError = errors::add))
            span.end()
            assertFailed(Slot(sequence.epoch(), 1L))
            val failure = errors.filterIsInstance<OtlpTransportException>().single()
            assertEquals(500, failure.statusCode)
            assertEquals("unavailable", failure.responseBody)
            assertTrue(failure.message!!.contains("unavailable"))
        }
    }

    @Test
    fun `a client-side refusal marks the slot failed and does not throw out of end`() {
        val errors = ArrayList<Throwable>()
        withSentinel(200, ByteArray(0)) { sentinel ->
            val span = start(sentinel, eventId = null, options = ExportOptions(synchronous = true, onError = errors::add))
            span.end()
            assertTrue(errors.single() is SpanExportRefusal)
            assertFailed(Slot(sequence.epoch(), 1L))
            assertEquals(0, sentinel.bodies.size)
        }
    }

    @Test
    fun `a host-stamped span keeps its slot`() {
        withSentinel(200, ByteArray(0)) { sentinel ->
            val reserved = sequence.allocate()
            val span = start(sentinel, slot = reserved)
            assertEquals(1L, sequence.highestAllocated(), "the processor did not allocate")
            assertEquals(reserved.number, (span as ReadableSpan).getAttribute(numberKey))
            span.end()
            assertEquals(1L, sequence.declaration().completeThrough)
        }
    }

    @Test
    fun `without a sequence the processor neither stamps nor needs start`() {
        withSentinel(200, ByteArray(0)) { sentinel ->
            val exporter = OtlpHttpExporter(sentinel.origin, "sagashop-payment")
            assertFalse(OtlpExportingSpanProcessor(exporter).isStartRequired)
            assertTrue(OtlpExportingSpanProcessor(exporter, sequence).isStartRequired)
        }
    }

    // assertFailed shows the slot was marked FAILED rather than left in flight: complete_through
    // stays below it, and markFiltered, which applies only to an in-flight slot, is a no-op.
    private fun assertFailed(slot: Slot) {
        assertEquals(0L, sequence.declaration().completeThrough)
        sequence.markFiltered(slot, 1L)
        assertEquals(emptyList<FilteredRange>(), sequence.declaration().filtered)
    }

    private fun start(
        sentinel: FakeSentinel,
        eventId: String? = "01f0c0ffee-charge",
        slot: Slot? = null,
        options: ExportOptions = ExportOptions(synchronous = true),
    ): Span {
        val provider =
            SdkTracerProvider
                .builder()
                .addSpanProcessor(OtlpExportingSpanProcessor(OtlpHttpExporter(sentinel.origin, "sagashop-payment"), sequence, options))
                .build()
        sentinel.providers.add(provider)
        val builder =
            provider
                .get("probe.test")
                .spanBuilder("sagashop.payment.charge.completed")
                .setStartTimestamp(1_700_000_000_000_000_000L, java.util.concurrent.TimeUnit.NANOSECONDS)
                .setAttribute(ATTRIBUTE_SCHEMA_VERSION, "sagashop-contract-v1")
                .setAttribute(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH, 0L)
                .setAttribute(ATTRIBUTE_CLAIMED_SENSITIVITY, Sensitivity.SENSITIVITY_INTERNAL.name)
        if (eventId != null) builder.setAttribute(ATTRIBUTE_EVENT_ID, eventId)
        if (slot != null) builder.setAttribute(epochKey, slot.epoch).setAttribute(numberKey, slot.number)
        return builder.startSpan()
    }

    private class FakeSentinel(
        val origin: String,
    ) {
        val bodies: MutableList<ByteArray> = java.util.Collections.synchronizedList(ArrayList())
        val providers: MutableList<SdkTracerProvider> = ArrayList()
    }

    private fun withSentinel(
        status: Int,
        response: ByteArray,
        block: (FakeSentinel) -> Unit,
    ) {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val sentinel = FakeSentinel("http://127.0.0.1:${server.address.port}")
        server.createContext("/") { exchange ->
            sentinel.bodies.add(exchange.requestBody.readBytes())
            exchange.sendResponseHeaders(status, if (response.isEmpty()) -1 else response.size.toLong())
            exchange.responseBody.use { it.write(response) }
        }
        server.start()
        try {
            block(sentinel)
        } finally {
            sentinel.providers.forEach { it.shutdown() }
            server.stop(0)
        }
    }
}
