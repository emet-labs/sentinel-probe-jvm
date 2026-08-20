package dev.emet.sentinel.probe.sdk.emission

import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.SourceCapability
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.sdk.trace.data.LinkData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.data.StatusData
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SpanToEventTests {
    private val startNanos = 1700000000123456789L

    private fun spanContext(
        traceHex: String,
        spanHex: String,
    ): SpanContext = SpanContext.create(traceHex, spanHex, TraceFlags.getDefault(), TraceState.getDefault())

    private val validParent: SpanContext = spanContext("0102030405060708090a0b0c0d0e0f10", "1112131415161718")
    private val validLink: SpanContext = spanContext("0102030405060708090a0b0c0d0e0f10", "2122232425262728")

    private fun span(
        name: String = "x",
        attributes: Attributes = Attributes.empty(),
        links: List<LinkData> = emptyList(),
        parent: SpanContext = SpanContext.getInvalid(),
        startEpochNanos: Long = startNanos,
    ): SpanData =
        TestSpanData
            .builder()
            .setName(name)
            .setKind(SpanKind.INTERNAL)
            .setSpanContext(validParent)
            .setParentSpanContext(parent)
            .setStartEpochNanos(startEpochNanos)
            .setEndEpochNanos(startEpochNanos + 1000)
            .setAttributes(attributes)
            .setLinks(links)
            .setStatus(StatusData.unset())
            .setHasEnded(true)
            .setTotalRecordedEvents(0)
            .setTotalRecordedLinks(links.size)
            .setTotalAttributeCount(attributes.size())
            .setResource(Resource.getDefault())
            .setInstrumentationScopeInfo(InstrumentationScopeInfo.empty())
            .build()

    private fun convert(span: SpanData) = spanToEvent(SpanConversion(span = span, schemaVersion = "sentinel.model.v1", eventID = "evt-1"))

    private fun attributeValue(
        event: dev.emet.sentinel.model.v1.ProducerEvent,
        key: String,
    ): AttributeValue {
        for (entry in event.attributesList) {
            if (entry.key == key) return entry.value
        }
        throw AssertionError("attribute \"$key\" not emitted; got ${event.attributesList}")
    }

    @Test
    fun `span name becomes event kind`() {
        val event = convert(span(name = "sagashop.order.placed"))
        assertEquals("sagashop.order.placed", event.kind)
    }

    @Test
    fun `event id and schema version pass through`() {
        val event = spanToEvent(SpanConversion(span = span(name = "x"), schemaVersion = "sentinel.model.v1", eventID = "evt-42"))
        assertEquals("evt-42", event.id)
        assertEquals("sentinel.model.v1", event.schemaVersion)
    }

    @Test
    fun `acknowledged epoch is stamped including zero`() {
        data class Case(
            val epoch: Long?,
            val want: Boolean,
            val name: String,
        )
        val cases =
            listOf(
                Case(0L, true, "epoch zero is stamped"),
                Case(7L, true, "a positive epoch is stamped"),
                Case(null, false, "an absent epoch stays absent"),
            )
        for (c in cases) {
            val event =
                spanToEvent(SpanConversion(span = span(name = "x"), schemaVersion = "v", eventID = "e", acknowledgedEpoch = c.epoch))
            assertEquals(c.want, event.hasAcknowledgedFilterEpoch(), c.name)
            if (c.want) assertEquals(c.epoch!!, event.acknowledgedFilterEpoch, c.name)
        }
    }

    @Test
    fun `sequence, capabilities and sensitivity pass through`() {
        val event =
            spanToEvent(
                SpanConversion(
                    span = span(name = "x"),
                    schemaVersion = "v",
                    eventID = "e",
                    sequence =
                        SequenceCoordinate
                            .newBuilder()
                            .setEpoch(3L)
                            .setSequence(9L)
                            .build(),
                    claimedCapabilities = listOf(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES),
                    claimedSensitivity = Sensitivity.SENSITIVITY_CONFIDENTIAL,
                ),
            )
        assertEquals(9L, event.sequence.sequence)
        assertEquals(3L, event.sequence.epoch)
        assertEquals(listOf(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES), event.claimedCapabilitiesList)
        assertEquals(Sensitivity.SENSITIVITY_CONFIDENTIAL, event.claimedSensitivity)
    }

    @Test
    fun `string attribute maps to string_value`() {
        val event = convert(span(attributes = Attributes.of(AttributeKey.stringKey("sagashop.order.id"), "ord-9")))
        assertEquals("ord-9", attributeValue(event, "sagashop.order.id").stringValue)
    }

    @Test
    fun `boolean attribute maps to bool_value`() {
        val event = convert(span(attributes = Attributes.of(AttributeKey.booleanKey("approved"), true)))
        val v = attributeValue(event, "approved")
        assertEquals(AttributeValue.ValueCase.BOOL_VALUE, v.valueCase)
        assertTrue(v.boolValue)
    }

    @Test
    fun `long attribute maps to integer_value`() {
        val event = convert(span(attributes = Attributes.of(AttributeKey.longKey("sagashop.amount_cents"), -4200L)))
        val v = attributeValue(event, "sagashop.amount_cents")
        assertEquals(AttributeValue.ValueCase.INTEGER_VALUE, v.valueCase)
        assertEquals(-4200L, v.integerValue)
    }

    @Test
    fun `double attribute maps to double_value even when integral`() {
        // Divergence D14: type-directed, so 3.0 stays a double. TypeScript is value-directed and
        // would emit integer_value. Kotlin/Java's behaviour is the correct one.
        val event =
            convert(
                span(
                    attributes =
                        Attributes.of(
                            AttributeKey.doubleKey("ratio"),
                            0.25,
                            AttributeKey.doubleKey("integral"),
                            3.0,
                        ),
                ),
            )
        assertEquals(0.25, attributeValue(event, "ratio").doubleValue)
        val integral = attributeValue(event, "integral")
        assertEquals(AttributeValue.ValueCase.DOUBLE_VALUE, integral.valueCase, "double 3.0 must stay a double")
    }

    @Test
    fun `homogeneous slices map to array_value`() {
        val event =
            convert(
                span(
                    attributes =
                        Attributes.of(
                            AttributeKey.stringArrayKey("tags"),
                            listOf("a", "b"),
                            AttributeKey.longArrayKey("counts"),
                            listOf(1L, 2L, 3L),
                            AttributeKey.doubleArrayKey("ratios"),
                            listOf(0.5),
                            AttributeKey.booleanArrayKey("flags"),
                            listOf(true, false),
                        ),
                ),
            )
        val tags = attributeValue(event, "tags").arrayValue
        assertEquals(2, tags.valuesList.size)
        assertEquals("b", tags.valuesList[1].stringValue)
        val counts = attributeValue(event, "counts").arrayValue
        assertEquals(3, counts.valuesList.size)
        assertEquals(3L, counts.valuesList[2].integerValue)
        val ratios = attributeValue(event, "ratios").arrayValue
        assertEquals(1, ratios.valuesList.size)
        assertEquals(0.5, ratios.valuesList[0].doubleValue)
        val flags = attributeValue(event, "flags").arrayValue
        assertEquals(2, flags.valuesList.size)
        assertTrue(flags.valuesList[0].boolValue)
        assertFalse(flags.valuesList[1].boolValue)
    }

    @Test
    fun `empty arrays map to empty containers`() {
        val event = convert(span(attributes = Attributes.of(AttributeKey.stringArrayKey("tags"), emptyList<String>())))
        assertEquals(0, attributeValue(event, "tags").arrayValue.valuesList.size)
    }

    @Test
    fun `reserved keys are excluded from attributes`() {
        val event =
            convert(
                span(
                    attributes =
                        Attributes.of(
                            AttributeKey.stringKey(ATTRIBUTE_EVENT_ID),
                            "evt-1",
                            AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID),
                            "evt-0",
                            AttributeKey.stringKey("kept"),
                            "v",
                        ),
                ),
            )
        for (entry in event.attributesList) {
            assertFalse(entry.key == ATTRIBUTE_EVENT_ID || entry.key == ATTRIBUTE_PARENT_EVENT_ID, "reserved key ${entry.key} leaked")
        }
        assertEquals(1, event.attributesList.size, "want only the domain attribute")
    }

    @Test
    fun `attribute order is canonical`() {
        // Divergence from the Go reference: OTel Java's Attributes canonicalises entries by key
        // (sorted), rather than preserving the span's insertion order as Go's ordered slice does.
        // The emitted order is therefore the canonical key order, deterministic across runs.
        val event =
            convert(
                span(
                    attributes =
                        Attributes.of(
                            AttributeKey.stringKey("z"),
                            "1",
                            AttributeKey.stringKey("a"),
                            "2",
                            AttributeKey.stringKey("m"),
                            "3",
                        ),
                ),
            )
        assertEquals(listOf("a", "m", "z"), event.attributesList.map { it.key })
    }

    @Test
    fun `parent event id comes from reserved attribute`() {
        val event =
            convert(
                span(
                    parent = validParent,
                    attributes = Attributes.of(AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID), "evt-parent"),
                ),
            )
        assertEquals(listOf("evt-parent"), event.causalPredecessorIdsList)
    }

    @Test
    fun `link predecessor comes from that link's own attributes`() {
        val link =
            LinkData.create(
                validLink,
                Attributes.of(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), "evt-linked"),
            )
        val event = convert(span(links = listOf(link)))
        assertEquals(listOf("evt-linked"), event.causalPredecessorIdsList)
    }

    @Test
    fun `parent and link produce exactly the joinable edges`() {
        // Parent first, then links in order.
        val link1 = LinkData.create(validLink, Attributes.of(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), "link-a"))
        val link2 = LinkData.create(validLink, Attributes.of(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), "link-b"))
        val event =
            convert(
                span(
                    parent = validParent,
                    attributes = Attributes.of(AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID), "evt-parent"),
                    links = listOf(link1, link2),
                ),
            )
        assertEquals(listOf("evt-parent", "link-a", "link-b"), event.causalPredecessorIdsList)
    }

    @Test
    fun `missing link event id falls back to span id and reports`() {
        val malformed = mutableListOf<Pair<String, String>>()
        val event =
            spanToEvent(
                SpanConversion(
                    span = span(links = listOf(LinkData.create(validLink, Attributes.empty()))),
                    schemaVersion = "v",
                    eventID = "e",
                    onMalformedLink = { source, spanID -> malformed.add(source to spanID) },
                ),
            )
        assertEquals(listOf("2122232425262728"), event.causalPredecessorIdsList, "fallback to the link's hex span id")
        assertEquals(listOf(MALFORMED_LINK_SOURCE_LINK to "2122232425262728"), malformed)
    }

    @Test
    fun `missing parent event id falls back to span id and reports`() {
        val malformed = mutableListOf<Pair<String, String>>()
        val event =
            spanToEvent(
                SpanConversion(
                    span = span(parent = validParent),
                    schemaVersion = "v",
                    eventID = "e",
                    onMalformedLink = { source, spanID -> malformed.add(source to spanID) },
                ),
            )
        assertEquals(listOf("1112131415161718"), event.causalPredecessorIdsList, "fallback to the parent's hex span id")
        assertEquals(listOf(MALFORMED_LINK_SOURCE_PARENT to "1112131415161718"), malformed)
    }

    @Test
    fun `mistyped reserved attribute takes the fallback path`() {
        // A non-string value under a reserved key is treated as absent, matching the reference's
        // typeof guard, rather than being coerced into a garbage event id.
        val malformed = mutableListOf<String>()
        val event =
            spanToEvent(
                SpanConversion(
                    span =
                        span(
                            parent = validParent,
                            attributes = Attributes.of(AttributeKey.longKey(ATTRIBUTE_PARENT_EVENT_ID), 7L),
                        ),
                    schemaVersion = "v",
                    eventID = "e",
                    onMalformedLink = { source, _ -> malformed.add(source) },
                ),
            )
        assertEquals(listOf("1112131415161718"), event.causalPredecessorIdsList)
        assertEquals(listOf(MALFORMED_LINK_SOURCE_PARENT), malformed)
        assertEquals(0, event.attributesList.size, "a reserved key stays excluded even when mistyped")
    }

    @Test
    fun `no parent and no links yields no predecessors`() {
        var called = false
        val event =
            spanToEvent(
                SpanConversion(
                    span = span(),
                    schemaVersion = "v",
                    eventID = "e",
                    onMalformedLink = { _, _ -> called = true },
                ),
            )
        assertEquals(0, event.causalPredecessorIdsList.size)
        assertFalse(called, "no edges means no malformed-link reports")
    }

    @Test
    fun `invalid parent is not an edge`() {
        // A root span yields an invalid parent SpanContext. isValid is the correct emptiness
        // test; a null check would not even compile (SpanContext is a value interface).
        val event = convert(span(parent = SpanContext.getInvalid()))
        assertEquals(0, event.causalPredecessorIdsList.size, "an invalid parent must not become an edge")
    }

    @Test
    fun `null onMalformedLink is safe`() {
        val event =
            spanToEvent(
                SpanConversion(
                    span = span(links = listOf(LinkData.create(validLink, Attributes.empty()))),
                    schemaVersion = "v",
                    eventID = "e",
                    onMalformedLink = null,
                ),
            )
        assertEquals(1, event.causalPredecessorIdsList.size, "the fallback must still be recorded without a callback")
    }

    @Test
    fun `probe tracer exposes a tracer and converts spans`() {
        // Divergence from the Go reference: this ProbeTracer accepts an EXISTING provider and
        // never starts its own (ADR-0002). The host owns the provider's lifecycle.
        val provider =
            io.opentelemetry.sdk.trace.SdkTracerProvider
                .builder()
                .build()
        try {
            val tracer = ProbeTracer.newProbeTracer(provider, "probe.test")
            assertNotNull(tracer.tracer)
            assertEquals(provider, tracer.provider)
            val conversion =
                SpanConversion(
                    span = span(attributes = Attributes.of(AttributeKey.stringKey("k"), "v")),
                    schemaVersion = "v",
                    eventID = "evt-1",
                )
            val viaMethod = tracer.toEvent(conversion)
            val viaFunc = spanToEvent(conversion)
            assertEquals(viaFunc.kind, viaMethod.kind)
            assertEquals(viaFunc.attributesList.size, viaMethod.attributesList.size)
        } finally {
            provider.shutdown()
        }
    }

    @Test
    fun `probe tracer does not own the provider lifecycle`() {
        // Divergence from the Go reference: ProbeTracer accepts an EXISTING provider and never
        // starts or shuts one (ADR-0002). The host owns the provider's lifecycle and resource;
        // this test documents that contract by constructing, using and shutting the provider on
        // the host side, with no shutdown call on the ProbeTracer itself.
        val provider =
            io.opentelemetry.sdk.trace.SdkTracerProvider
                .builder()
                .build()
        val tracer = ProbeTracer.newProbeTracer(provider, "probe.test")
        assertNotNull(tracer.tracer)
        // The SDK never attaches a span processor or exporter: there is nothing to flush.
        assertTrue(provider.forceFlush().isSuccess)
        assertTrue(provider.shutdown().isSuccess)
    }
}
