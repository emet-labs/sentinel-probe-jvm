package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_CAPABILITIES
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.sdk.emission.MissingPredecessorId
import dev.emet.sentinel.probe.sdk.emission.SpanConversion
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import dev.emet.sentinel.probe.sdk.emission.spanToEvent
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.int
import io.kotest.property.arbitrary.long
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
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
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class OtlpExportPropertyTests {
    // Same pinned seed as ApplyFilterPropertyTests. The draw is the seed, not a wall clock.
    private val config = PropTestConfig(seed = 31L)
    private val startNanos = 1_700_000_000_000_000_000L
    private val parentHex = "1112131415161718"
    private val linkHex = "2122232425262728"
    private val parentContext =
        SpanContext.create("0102030405060708090a0b0c0d0e0f10", parentHex, TraceFlags.getDefault(), TraceState.getDefault())
    private val linkContext =
        SpanContext.create("0102030405060708090a0b0c0d0e0f10", linkHex, TraceFlags.getDefault(), TraceState.getDefault())
    private val spanContext =
        SpanContext.create("0102030405060708090a0b0c0d0e0f10", "3132333435363738", TraceFlags.getDefault(), TraceState.getDefault())

    @Test
    fun `random attributes match spanToEvent or the export is refused`() {
        runBlocking {
            checkAll(config, Arb.int(0..255), Arb.string(1, 6), Arb.long(-2L..4L)) { bits, word, number ->
                val withParent = bits and 1 != 0
                val missingParentId = bits and 2 != 0
                val withLink = bits and 4 != 0
                val missingLinkId = bits and 8 != 0
                val unknown = bits and 16 != 0
                val nan = bits and 32 != 0
                val omitSequence = bits and 64 != 0
                val omitSensitivity = bits and 128 != 0
                val parentId = if (withParent && !missingParentId) "parent-$word" else null
                val linkId = if (withLink && !missingLinkId) "link-$word" else null
                val span =
                    span(
                        attributes =
                            attributes(
                                word = word,
                                number = number,
                                parentId = parentId,
                                unknown = unknown,
                                nan = nan,
                                omitSequence = omitSequence,
                                omitSensitivity = omitSensitivity,
                            ),
                        parent = if (withParent) parentContext else SpanContext.getInvalid(),
                        links =
                            if (withLink) {
                                listOf(
                                    LinkData.create(
                                        linkContext,
                                        if (linkId == null) {
                                            Attributes.empty()
                                        } else {
                                            Attributes.of(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), linkId)
                                        },
                                    ),
                                )
                            } else {
                                emptyList()
                            },
                    )
                val refused =
                    unknown || nan || omitSequence || omitSensitivity || (withParent && missingParentId) || (withLink && missingLinkId)
                if (refused) {
                    assertThrows<SpanExportRefusal> { OtlpCodec.encodeTrace(listOf(span)) }
                    return@checkAll
                }
                val decoded = OtlpCodec.decodeTraceEvents(OtlpCodec.encodeTrace(listOf(span))).single()
                val direct =
                    spanToEvent(
                        SpanConversion(
                            span = span,
                            sequence =
                                SequenceCoordinate
                                    .newBuilder()
                                    .setEpoch(7)
                                    .setSequence(1)
                                    .build(),
                            schemaVersion = "sagashop-contract-v1",
                            acknowledgedEpoch = 4,
                            claimedCapabilities = listOf(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES),
                            claimedSensitivity = Sensitivity.SENSITIVITY_INTERNAL,
                            eventID = "evt-$word",
                        ),
                    )
                assertEquals(direct, decoded)
                for (id in decoded.causalPredecessorIdsList) {
                    assertNotEquals(parentHex, id)
                    assertNotEquals(linkHex, id)
                }
                if (parentId != null) assertTrue(decoded.causalPredecessorIdsList.contains(parentId))
                if (linkId != null) assertTrue(decoded.causalPredecessorIdsList.contains(linkId))
            }
        }
    }

    @Test
    fun `a missing predecessor is not encoded as its span id`() {
        val span =
            span(
                attributes =
                    attributes(
                        word = "x",
                        number = 1,
                        parentId = null,
                        unknown = false,
                        nan = false,
                        omitSequence = false,
                        omitSensitivity = false,
                    ),
                parent = parentContext,
            )
        val error = assertThrows<MissingPredecessorId> { OtlpCodec.encodeTrace(listOf(span)) }
        assertEquals(parentHex, error.spanId)
    }

    private fun attributes(
        word: String,
        number: Long,
        parentId: String?,
        unknown: Boolean,
        nan: Boolean,
        omitSequence: Boolean,
        omitSensitivity: Boolean,
    ): Attributes {
        val builder = Attributes.builder()
        builder.put(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), "evt-$word")
        builder.put(AttributeKey.stringKey(ATTRIBUTE_SCHEMA_VERSION), "sagashop-contract-v1")
        if (!omitSequence) {
            builder.put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH), 7L)
            builder.put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER), 1L)
        }
        builder.put(AttributeKey.longKey(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH), 4L)
        if (!omitSensitivity) {
            builder.put(AttributeKey.stringKey(ATTRIBUTE_CLAIMED_SENSITIVITY), "SENSITIVITY_INTERNAL")
        }
        builder.put(
            AttributeKey.stringArrayKey(ATTRIBUTE_CLAIMED_CAPABILITIES),
            listOf("SOURCE_CAPABILITY_CAUSAL_EDGES"),
        )
        builder.put(AttributeKey.stringKey("domain.$word"), word)
        builder.put(AttributeKey.longKey("domain.n"), number)
        builder.put(AttributeKey.booleanKey("domain.flag"), number >= 0)
        if (parentId != null) builder.put(AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID), parentId)
        if (unknown) builder.put(AttributeKey.stringKey("sentinel.extra"), "nope")
        if (nan) builder.put(AttributeKey.doubleKey("domain.ratio"), Double.NaN)
        return builder.build()
    }

    private fun span(
        attributes: Attributes,
        parent: SpanContext,
        links: List<LinkData> = emptyList(),
    ): SpanData =
        TestSpanData
            .builder()
            .setName("sagashop.order.ship.requested")
            .setKind(SpanKind.INTERNAL)
            .setSpanContext(spanContext)
            .setParentSpanContext(parent)
            .setStartEpochNanos(startNanos)
            .setEndEpochNanos(startNanos + 5_000_000)
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
}
