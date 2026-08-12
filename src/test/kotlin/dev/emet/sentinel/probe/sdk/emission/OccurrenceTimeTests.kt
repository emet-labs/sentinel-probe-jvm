package dev.emet.sentinel.probe.sdk.emission

import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.data.StatusData
import org.junit.jupiter.api.Test
import java.math.BigInteger
import kotlin.test.assertEquals

class OccurrenceTimeTests {
    private val ctx: SpanContext =
        SpanContext.create("0102030405060708090a0b0c0d0e0f10", "1112131415161718", TraceFlags.getDefault(), TraceState.getDefault())

    private fun spanAt(startNanos: Long): io.opentelemetry.sdk.trace.data.SpanData =
        TestSpanData.builder()
            .setName("x")
            .setKind(SpanKind.INTERNAL)
            .setSpanContext(ctx)
            .setParentSpanContext(SpanContext.getInvalid())
            .setStartEpochNanos(startNanos)
            .setEndEpochNanos(startNanos + 1_000_000L)
            .setAttributes(Attributes.empty())
            .setLinks(emptyList())
            .setStatus(StatusData.unset())
            .setHasEnded(true)
            .setTotalRecordedEvents(0)
            .setTotalRecordedLinks(0)
            .setTotalAttributeCount(0)
            .setResource(Resource.getDefault())
            .setInstrumentationScopeInfo(InstrumentationScopeInfo.empty())
            .build()

    @Test
    fun `occurrence time uses the unix clock domain and zero uncertainty`() {
        val occurrence = buildOccurrenceTime(spanAt(1700000000123456789L))
        assertEquals("unix", occurrence.clockDomainId)
        // Always zero, mirroring span-to-event.ts:97-103 exactly. There is no Probe-side input
        // for SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY and SpanConversion carries no field.
        assertEquals(0L, occurrence.uncertaintyNanoseconds)
        assertEquals(0, Int128Codec.toBigInt(occurrence.nanoseconds).compareTo(BigInteger("1700000000123456789")))
    }

    @Test
    fun `occurrence time comes from start not end`() {
        val occurrence = buildOccurrenceTime(spanAt(1700000000000000000L))
        assertEquals(0, Int128Codec.toBigInt(occurrence.nanoseconds).compareTo(BigInteger("1700000000000000000")), "want the START time")
    }

    @Test
    fun `occurrence time before the epoch is sign-extended`() {
        // One half-second before the Unix epoch: -500_000_000 ns. Must decode as a negative value,
        // not a wrapped unsigned word.
        val occurrence = buildOccurrenceTime(spanAt(-500_000_000L))
        assertEquals(0, Int128Codec.toBigInt(occurrence.nanoseconds).compareTo(BigInteger.valueOf(-500_000_000L)), "sign extension, not a wrapped unsigned word")
        assertEquals(-1L, occurrence.nanoseconds.high, "high = -1 for a pre-epoch time")
    }

    @Test
    fun `spanToEvent carries the occurrence time`() {
        val event = spanToEvent(SpanConversion(span = spanAt(1700000000000000001L), schemaVersion = "v", eventID = "e"))
        val occurrence = event.occurrenceTime
        assertEquals(0, Int128Codec.toBigInt(occurrence.nanoseconds).compareTo(BigInteger("1700000000000000001")))
    }
}
