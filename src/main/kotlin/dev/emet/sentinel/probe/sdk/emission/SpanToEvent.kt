// Package emission converts OTel Java spans into sentinel.model.v1.ProducerEvent values.
// Kotlin analog of sdk/go/emission/ (spantoevent.go, tracer.go).
//
// The SDK does not own export. ADR-0002 makes OTel an adapter, not the substrate: this package
// produces spans and converts them, and the host wires the OTLP exporter.
//
// # Producer contract for causal edges
//
// ProducerEvent.causal_predecessor_ids carries event IDs, not span IDs, so the producer must
// thread them through span attributes:
//
//   - at span start, the producer assigns an event ID and sets it as the span attribute
//     "sentinel.event.id";
//   - when linking to a predecessor span A, the producer stamps A's event ID into THAT LINK'S
//     OWN attributes under "sentinel.event.id";
//   - for the parent, the producer threads the parent's event ID into the child span's
//     attributes under "sentinel.parent.event.id".
//
// spanToEvent reads predecessors from exactly those places. It falls back to the hex span ID
// only when the attribute is absent, and reports every such fallback through
// SpanConversion.onMalformedLink, because a fallback means the producer contract was violated
// and the resulting edge is not joinable with anything else. Both reserved keys are excluded
// from ProducerEvent.attributes: they carry causal-edge metadata, not domain data.
//
// # Attribute mapping
//
// OTel Java 1.47.0's AttributeType set is STRING, BOOLEAN, LONG, DOUBLE and the four homogeneous
// array types (STRING_ARRAY, BOOLEAN_ARRAY, LONG_ARRAY, DOUBLE_ARRAY), so five of the seven
// AttributeValue oneof arms are reachable natively:
//
//   STRING      -> string_value
//   BOOLEAN     -> bool_value
//   LONG        -> integer_value
//   DOUBLE      -> double_value
//   *_ARRAY     -> array_value (homogeneous, recursive)
//
// bytes_value and map_value are NOT reachable: OTel Java has no bytes or map attribute type at
// this version, so there is no branch to write. This is a divergence from the Go/TypeScript
// references (whose OTel runtimes expose bytes, heterogeneous slice and map), documented here
// rather than papered over with a fake mapping.
//
// Divergence D14: integer/double dispatch is TYPE-directed here and VALUE-directed in
// TypeScript. span-to-event.ts branches on Number.isInteger(value), so JavaScript 3 and 3.0 are
// the same value and both become integer_value. Kotlin/Java dispatches on LONG versus DOUBLE,
// so 3.0 stays a double_value. This is the correct behaviour and is not a bug to be reported as
// drift.
package dev.emet.sentinel.probe.sdk.emission

import dev.emet.sentinel.model.v1.AttributeArray
import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeMap
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.sdk.trace.data.LinkData
import io.opentelemetry.sdk.trace.data.SpanData

// Reserved attribute keys. They carry causal-edge metadata and are never emitted as
// ProducerEvent attributes. Same string constants as the TypeScript/Go references.
public const val ATTRIBUTE_EVENT_ID: String = "sentinel.event.id"
public const val ATTRIBUTE_PARENT_EVENT_ID: String = "sentinel.parent.event.id"

// Malformed-link sources reported through SpanConversion.onMalformedLink.
public const val MALFORMED_LINK_SOURCE_PARENT: String = "parent"
public const val MALFORMED_LINK_SOURCE_LINK: String = "link"

// SpanConversion is everything spanToEvent needs beyond the span itself. Named SpanConversion
// rather than SpanContext (the TypeScript name) because OTel's SpanContext appears in this very
// conversion — SpanData.getParentSpanContext() returns one and every LinkData carries one —
// and shadowing it would be actively confusing.
public data class SpanConversion(
    // span is the ended span to convert. In tests, build it with
    // io.opentelemetry.sdk.testing.trace.TestSpanData.builder(); SpanData is a sealed-ish
    // interface that is awkward to implement by hand.
    public val span: SpanData,
    // sequence is the host-assigned sequence coordinate. Sequence assignment is out of SDK
    // scope: Probes are thin (ADR-0006).
    public val sequence: SequenceCoordinate? = null,
    // schemaVersion of the emitted event.
    public val schemaVersion: String,
    // acknowledgedEpoch is the filter epoch this Probe has acknowledged, typically
    // ProbeClient.acknowledgedEpoch(). null means none is held; it does not mean 0.
    public val acknowledgedEpoch: Long? = null,
    // claimedCapabilities the source asserts. Claims only: the receiver owns effective
    // capabilities, tier, integrity and observation time by construction (event.proto:32-33).
    public val claimedCapabilities: List<SourceCapability> = emptyList(),
    // claimedSensitivity of the event.
    public val claimedSensitivity: Sensitivity = Sensitivity.SENSITIVITY_UNSPECIFIED,
    // eventID is the reserved sentinel.event.id the producer assigned at span start.
    public val eventID: String,
    // onMalformedLink, when non-null, is called once per causal predecessor that had to fall
    // back to a hex span ID because the producer contract was violated. source is
    // MALFORMED_LINK_SOURCE_PARENT or MALFORMED_LINK_SOURCE_LINK.
    public val onMalformedLink: ((source: String, spanID: String) -> Unit)? = null,
)

// spanToEvent converts an ended OTel span into a ProducerEvent.
//
// The event's kind is the span name, its occurrence time is the span's start time, its
// attributes are the span's attributes minus the two reserved keys, and its causal predecessors
// come from the parent and link attributes described in the package doc.
public fun spanToEvent(conversion: SpanConversion): ProducerEvent {
    val span = conversion.span
    val builder = ProducerEvent.newBuilder()
        .setId(conversion.eventID)
        .setSchemaVersion(conversion.schemaVersion)
        .setKind(span.name)
        .setOccurrenceTime(buildOccurrenceTime(span))
        .addAllAttributes(mapAttributes(span.attributes))
        .setClaimedSensitivity(conversion.claimedSensitivity)
        .addAllCausalPredecessorIds(collectCausalPredecessors(span, conversion.onMalformedLink))
    if (conversion.sequence != null) builder.setSequence(conversion.sequence)
    if (conversion.acknowledgedEpoch != null) builder.setAcknowledgedFilterEpoch(conversion.acknowledgedEpoch)
    if (conversion.claimedCapabilities.isNotEmpty()) builder.addAllClaimedCapabilities(conversion.claimedCapabilities)
    return builder.build()
}

// buildOccurrenceTime converts a span's start time into an OccurrenceTime in the "unix" clock
// domain.
//
// uncertainty_nanoseconds is always 0, mirroring span-to-event.ts:97-103 exactly. There is no
// Probe-side input for SOURCE_CAPABILITY_BOUNDED_CLOCK_UNCERTAINTY and SpanConversion
// deliberately carries no field to supply one, so there is no branch to write.
//
// OTel Java's getStartEpochNanos() is already nanoseconds since the Unix epoch as a long, so
// fromInt64 is exact for every representable instant; the BigInteger path in Int128Codec exists
// for the beyond-int64 occurrence times the Go/TypeScript references test, which OTel Java's
// long-valued clock cannot express.
public fun buildOccurrenceTime(span: SpanData): OccurrenceTime =
    OccurrenceTime.newBuilder()
        .setClockDomainId("unix")
        .setNanoseconds(Int128Codec.fromInt64(span.startEpochNanos))
        .setUncertaintyNanoseconds(0)
        .build()

private fun mapAttributes(attributes: Attributes): List<AttributeEntry> {
    val entries = ArrayList<AttributeEntry>(attributes.size())
    attributes.forEach { key, value ->
        val name = key.key
        if (name == ATTRIBUTE_EVENT_ID || name == ATTRIBUTE_PARENT_EVENT_ID) return@forEach // reserved
        val mapped = mapValue(key.type, value) ?: return@forEach // EMPTY / unmapped -> skip
        entries.add(AttributeEntry.newBuilder().setKey(name).setValue(mapped).build())
    }
    return entries
}

// mapValue maps one attribute value onto the AttributeValue oneof, dispatching by the OTel
// AttributeType. Returns null for unmapped values, mirroring the reference skipping null.
@Suppress("UNCHECKED_CAST")
private fun mapValue(type: AttributeType, value: Any?): AttributeValue? = when (type) {
    AttributeType.STRING -> AttributeValue.newBuilder().setStringValue(value as String).build()
    AttributeType.BOOLEAN -> AttributeValue.newBuilder().setBoolValue(value as Boolean).build()
    AttributeType.LONG -> AttributeValue.newBuilder().setIntegerValue(value as Long).build()
    AttributeType.DOUBLE -> AttributeValue.newBuilder().setDoubleValue(value as Double).build()
    AttributeType.STRING_ARRAY -> arrayValue((value as List<String>).map { mapValue(AttributeType.STRING, it) })
    AttributeType.BOOLEAN_ARRAY -> arrayValue((value as List<Boolean>).map { mapValue(AttributeType.BOOLEAN, it) })
    AttributeType.LONG_ARRAY -> arrayValue((value as List<Long>).map { mapValue(AttributeType.LONG, it) })
    AttributeType.DOUBLE_ARRAY -> arrayValue((value as List<Double>).map { mapValue(AttributeType.DOUBLE, it) })
}

private fun arrayValue(values: List<AttributeValue?>): AttributeValue {
    val cleaned = values.filterNotNull()
    return AttributeValue.newBuilder()
        .setArrayValue(
            AttributeArray.newBuilder().addAllValues(cleaned).build(),
        )
        .build()
}

// collectCausalPredecessors reads the parent edge and every link edge, in that order.
private fun collectCausalPredecessors(
    span: SpanData,
    onMalformedLink: ((source: String, spanID: String) -> Unit)?,
): List<String> {
    val predecessors = ArrayList<String>()

    // Parent: the child span carries the parent's event ID under the reserved key.
    val parentEventID = stringAttribute(span.attributes, ATTRIBUTE_PARENT_EVENT_ID)
    if (parentEventID != null) {
        predecessors.add(parentEventID)
    } else {
        val parent = span.parentSpanContext
        if (parent.isValid) {
            val spanID = parent.spanId
            predecessors.add(spanID)
            onMalformedLink?.invoke(MALFORMED_LINK_SOURCE_PARENT, spanID)
        }
    }

    // Links: each link carries its target's event ID in that link's OWN attributes.
    for (link in span.links) {
        val linkEventID = stringAttribute(link.attributes, ATTRIBUTE_EVENT_ID)
        if (linkEventID != null) {
            predecessors.add(linkEventID)
            continue
        }
        val spanID = link.spanContext.spanId
        predecessors.add(spanID)
        onMalformedLink?.invoke(MALFORMED_LINK_SOURCE_LINK, spanID)
    }
    return predecessors
}

// stringAttribute returns the value of key when it is present AND holds a string. A non-string
// value under a reserved key is treated as absent, matching the reference's `typeof x ===
// "string"` guard, so a mistyped attribute takes the malformed-link path rather than silently
// producing a garbage event ID.
private fun stringAttribute(attributes: Attributes, key: String): String? {
    var found: String? = null
    attributes.forEach { k, v ->
        if (k.key == key && k.type == AttributeType.STRING) {
            found = v as String
        }
    }
    return found
}
