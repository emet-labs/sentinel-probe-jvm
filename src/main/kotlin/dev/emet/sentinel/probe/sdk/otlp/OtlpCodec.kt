// OTLP/HTTP protobuf codec for the Probe wire (ADR-0002 addendum).
//
// Traces carry ended spans. Logs carry a ProducerCompletenessSignal out, and
// Verdict, EventFilter, and DecideResponse back in. Span ids are never causal
// ids. A missing predecessor, an unknown sentinel.* key, a non-finite double,
// or a dropped attribute fails the whole export.
package dev.emet.sentinel.probe.sdk.otlp

import com.google.protobuf.ByteString
import com.google.protobuf.InvalidProtocolBufferException
import dev.emet.sentinel.model.v1.AttributeArray
import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeMap
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.model.v1.Verdict
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_CAPABILITIES
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.sdk.emission.MALFORMED_LINK_SOURCE_LINK
import dev.emet.sentinel.probe.sdk.emission.MALFORMED_LINK_SOURCE_PARENT
import dev.emet.sentinel.probe.sdk.emission.MissingPredecessorId
import dev.emet.sentinel.probe.sdk.emission.SpanConversion
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import dev.emet.sentinel.probe.sdk.emission.spanToEvent
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import io.opentelemetry.api.common.AttributeType
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.common.v1.ArrayValue
import io.opentelemetry.proto.common.v1.KeyValue
import io.opentelemetry.proto.common.v1.KeyValueList
import io.opentelemetry.proto.logs.v1.LogRecord
import io.opentelemetry.proto.logs.v1.ResourceLogs
import io.opentelemetry.proto.logs.v1.ScopeLogs
import io.opentelemetry.proto.trace.v1.ResourceSpans
import io.opentelemetry.proto.trace.v1.ScopeSpans
import io.opentelemetry.proto.trace.v1.Span
import io.opentelemetry.sdk.trace.data.EventData
import io.opentelemetry.sdk.trace.data.LinkData
import io.opentelemetry.sdk.trace.data.SpanData

public const val SOURCE_HANDLE_HEADER: String = "x-sentinel-source-handle"
public const val TRACES_PATH: String = "/v1/traces"
public const val LOGS_PATH: String = "/v1/logs"
public const val PROTOBUF_MEDIA_TYPE: String = "application/x-protobuf"
public const val COMPLETENESS_PAYLOAD_TYPE: String = "sentinel.model.v1.ProducerCompletenessSignal"
public const val EVENT_FILTER_PAYLOAD_TYPE: String = "sentinel.model.v1.EventFilter"
public const val VERDICT_PAYLOAD_TYPE: String = "sentinel.model.v1.Verdict"
public const val DECIDE_RESPONSE_PAYLOAD_TYPE: String = "sentinel.probe.v1.DecideResponse"
public const val PAYLOAD_TYPE_ATTRIBUTE: String = "sentinel.payload.type"
public const val REQUEST_ID_ATTRIBUTE: String = "sentinel.request_id"
public const val SOURCE_HANDLE_ATTRIBUTE: String = "sentinel.source_handle"

private const val GZIP_MAGIC_0: Byte = 0x1f
private const val GZIP_MAGIC_1: Byte = 0x8b.toByte()

// LogDecodeException is a log export the Probe receiver will not apply.
public open class LogDecodeException(
    message: String,
    cause: Throwable? = null,
) : IllegalArgumentException(message, cause)

// UnspecifiedDecisionAction is a DecideResponse whose action is not permit, deny, or defer.
// requestId is empty when the message itself had none. The gate does not treat this as a permit.
public class UnspecifiedDecisionAction(
    public val requestId: String,
) : LogDecodeException("DecideResponse action must not be unspecified")

internal sealed class InboundLog {
    class VerdictLog(
        val verdict: Verdict,
    ) : InboundLog()

    class FilterLog(
        val filter: EventFilter,
    ) : InboundLog()

    class DecisionLog(
        val response: DecideResponse,
    ) : InboundLog()
}

@Suppress("TooManyFunctions")
internal object OtlpCodec {
    fun encodeTrace(spans: List<SpanData>): ByteArray {
        val encoded = spans.map { encodeSpan(it) }
        return ExportTraceServiceRequest
            .newBuilder()
            .addResourceSpans(
                ResourceSpans
                    .newBuilder()
                    .addScopeSpans(
                        ScopeSpans.newBuilder().addAllSpans(encoded).build(),
                    ).build(),
            ).build()
            .toByteArray()
    }

    fun decodeTraceEvents(bytes: ByteArray): List<ProducerEvent> {
        rejectWrapped(bytes)
        val request =
            try {
                ExportTraceServiceRequest.parseFrom(bytes)
            } catch (error: InvalidProtocolBufferException) {
                throw SpanExportRefusal("OTLP trace export is not valid protobuf: ${error.message}", error)
            }
        val events = ArrayList<ProducerEvent>()
        for (resource in request.resourceSpansList) {
            if (resource.hasResource()) {
                rejectDropped(resource.resource.droppedAttributesCount, "resource")
                rejectNonFinite(resource.resource.attributesList)
            }
            for (scope in resource.scopeSpansList) {
                if (scope.hasScope()) {
                    rejectDropped(scope.scope.droppedAttributesCount, "scope")
                    rejectNonFinite(scope.scope.attributesList)
                }
                for (span in scope.spansList) {
                    events.add(eventFromWireSpan(span))
                }
            }
        }
        return events
    }

    fun encodeCompleteness(
        signal: ProducerCompletenessSignal,
        sourceHandle: String,
    ): ByteArray {
        if (sourceHandle.isBlank()) throw SpanExportRefusal("source handle is blank")
        val record =
            LogRecord
                .newBuilder()
                .setBody(bytesValue(signal.toByteArray()))
                .addAttributes(stringKv(PAYLOAD_TYPE_ATTRIBUTE, COMPLETENESS_PAYLOAD_TYPE))
                .addAttributes(stringKv(SOURCE_HANDLE_ATTRIBUTE, sourceHandle))
                .build()
        return logsRequest(record)
    }

    fun encodeVerdict(verdict: Verdict): ByteArray = outboundLog(VERDICT_PAYLOAD_TYPE, verdict.toByteArray(), null)

    fun encodeFilter(filter: EventFilter): ByteArray = outboundLog(EVENT_FILTER_PAYLOAD_TYPE, filter.toByteArray(), null)

    fun encodeDecision(response: DecideResponse): ByteArray =
        outboundLog(DECIDE_RESPONSE_PAYLOAD_TYPE, response.toByteArray(), response.requestId)

    fun decodeInbound(bytes: ByteArray): List<InboundLog> {
        rejectWrapped(bytes)
        val request =
            try {
                ExportLogsServiceRequest.parseFrom(bytes)
            } catch (error: InvalidProtocolBufferException) {
                throw LogDecodeException("OTLP log export is not valid protobuf: ${error.message}", error)
            }
        return logRecords(request).map { decodeRecord(it) }
    }

    private fun logRecords(request: ExportLogsServiceRequest): List<LogRecord> {
        val records = ArrayList<LogRecord>()
        for (resource in request.resourceLogsList) {
            rejectLogResource(resource)
            for (scope in resource.scopeLogsList) {
                rejectLogScope(scope)
                records.addAll(scope.logRecordsList)
            }
        }
        return records
    }

    private fun rejectLogResource(resource: io.opentelemetry.proto.logs.v1.ResourceLogs) {
        if (!resource.hasResource()) return
        if (resource.resource.droppedAttributesCount != 0) {
            throw LogDecodeException("dropped resource attributes")
        }
        rejectNonFinite(resource.resource.attributesList)
    }

    private fun rejectLogScope(scope: ScopeLogs) {
        if (!scope.hasScope()) return
        if (scope.scope.droppedAttributesCount != 0) {
            throw LogDecodeException("dropped scope attributes")
        }
        rejectNonFinite(scope.scope.attributesList)
    }

    private fun encodeSpan(span: SpanData): Span {
        refuseIncomplete(span)
        val conversion = conversionFromSpan(span)
        val event = spanToEvent(conversion)
        val builder =
            Span
                .newBuilder()
                .setTraceId(ByteString.copyFrom(hexToBytes(span.spanContext.traceId)))
                .setSpanId(ByteString.copyFrom(hexToBytes(span.spanContext.spanId)))
                .setName(event.kind)
                .setStartTimeUnixNano(span.startEpochNanos)
                .setEndTimeUnixNano(span.endEpochNanos)
        if (span.parentSpanContext.isValid) {
            builder.setParentSpanId(ByteString.copyFrom(hexToBytes(span.parentSpanContext.spanId)))
        }
        builder.addAllAttributes(contractAttributes(event, span))
        for (entry in event.attributesList) {
            builder.addAttributes(keyValue(entry.key, anyFromAttribute(entry.value)))
        }
        for (link in span.links) {
            builder.addLinks(encodeLink(link))
        }
        for (eventData in span.events) {
            builder.addEvents(encodeSpanEvent(eventData))
        }
        return builder.build()
    }

    private fun conversionFromSpan(span: SpanData): SpanConversion {
        val eventId =
            stringAttribute(span.attributes, ATTRIBUTE_EVENT_ID)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_EVENT_ID")
        if (eventId.isBlank()) throw SpanExportRefusal("blank $ATTRIBUTE_EVENT_ID")
        val schema =
            stringAttribute(span.attributes, ATTRIBUTE_SCHEMA_VERSION)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_SCHEMA_VERSION")
        if (schema.isBlank()) throw SpanExportRefusal("blank $ATTRIBUTE_SCHEMA_VERSION")
        val epoch =
            longAttribute(span.attributes, ATTRIBUTE_SEQUENCE_EPOCH)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_SEQUENCE_EPOCH")
        val number =
            longAttribute(span.attributes, ATTRIBUTE_SEQUENCE_NUMBER)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_SEQUENCE_NUMBER")
        if (epoch < 0 || number < 0) throw SpanExportRefusal("sequence must be zero or positive")
        val filterEpoch =
            longAttribute(span.attributes, ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH")
        if (filterEpoch < 0) throw SpanExportRefusal("acknowledged filter epoch must be zero or positive")
        val sensitivityName =
            stringAttribute(span.attributes, ATTRIBUTE_CLAIMED_SENSITIVITY)
                ?: throw SpanExportRefusal("missing $ATTRIBUTE_CLAIMED_SENSITIVITY")
        val sensitivity =
            enumByName(Sensitivity.values(), sensitivityName)
                ?: throw SpanExportRefusal("unknown claimed sensitivity $sensitivityName")
        if (sensitivity == Sensitivity.SENSITIVITY_UNSPECIFIED) {
            throw SpanExportRefusal("claimed sensitivity must not be unspecified")
        }
        return SpanConversion(
            span = span,
            sequence =
                SequenceCoordinate
                    .newBuilder()
                    .setEpoch(epoch)
                    .setSequence(number)
                    .build(),
            schemaVersion = schema,
            acknowledgedEpoch = filterEpoch,
            claimedCapabilities = capabilitiesFrom(span.attributes),
            claimedSensitivity = sensitivity,
            eventID = eventId,
        )
    }

    private fun contractAttributes(
        event: ProducerEvent,
        span: SpanData,
    ): List<KeyValue> {
        val attributes = ArrayList<KeyValue>()
        attributes.add(stringKv(ATTRIBUTE_EVENT_ID, event.id))
        attributes.add(stringKv(ATTRIBUTE_SCHEMA_VERSION, event.schemaVersion))
        if (!event.hasSequence()) throw SpanExportRefusal("missing sequence")
        attributes.add(intKv(ATTRIBUTE_SEQUENCE_EPOCH, event.sequence.epoch))
        attributes.add(intKv(ATTRIBUTE_SEQUENCE_NUMBER, event.sequence.sequence))
        if (!event.hasAcknowledgedFilterEpoch()) throw SpanExportRefusal("missing acknowledged filter epoch")
        attributes.add(intKv(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH, event.acknowledgedFilterEpoch))
        if (event.claimedSensitivity == Sensitivity.SENSITIVITY_UNSPECIFIED ||
            event.claimedSensitivity == Sensitivity.UNRECOGNIZED
        ) {
            throw SpanExportRefusal("claimed sensitivity must not be unspecified")
        }
        attributes.add(stringKv(ATTRIBUTE_CLAIMED_SENSITIVITY, event.claimedSensitivity.name))
        if (event.claimedCapabilitiesCount > 0) {
            val array = ArrayValue.newBuilder()
            for (capability in event.claimedCapabilitiesList) {
                if (capability == SourceCapability.SOURCE_CAPABILITY_UNSPECIFIED ||
                    capability == SourceCapability.UNRECOGNIZED
                ) {
                    throw SpanExportRefusal("claimed capability must not be unspecified")
                }
                array.addValues(AnyValue.newBuilder().setStringValue(capability.name).build())
            }
            attributes.add(
                keyValue(ATTRIBUTE_CLAIMED_CAPABILITIES, AnyValue.newBuilder().setArrayValue(array).build()),
            )
        }
        val parent = stringAttribute(span.attributes, ATTRIBUTE_PARENT_EVENT_ID)
        if (parent != null) attributes.add(stringKv(ATTRIBUTE_PARENT_EVENT_ID, parent))
        return attributes
    }

    private fun encodeLink(link: LinkData): Span.Link {
        val eventId =
            stringAttribute(link.attributes, ATTRIBUTE_EVENT_ID)
                ?: throw MissingPredecessorId(MALFORMED_LINK_SOURCE_LINK, link.spanContext.spanId)
        val builder =
            Span.Link
                .newBuilder()
                .setTraceId(ByteString.copyFrom(hexToBytes(link.spanContext.traceId)))
                .setSpanId(ByteString.copyFrom(hexToBytes(link.spanContext.spanId)))
                .addAttributes(stringKv(ATTRIBUTE_EVENT_ID, eventId))
        link.attributes.forEach { key, value ->
            if (key.key == ATTRIBUTE_EVENT_ID) return@forEach
            if (key.key.startsWith("sentinel.")) throw SpanExportRefusal("unknown sentinel attribute ${key.key}")
            builder.addAttributes(keyValue(key.key, anyFromOtel(key.type, value)))
        }
        return builder.build()
    }

    private fun encodeSpanEvent(event: EventData): Span.Event {
        val builder =
            Span.Event
                .newBuilder()
                .setTimeUnixNano(event.epochNanos)
                .setName(event.name)
        event.attributes.forEach { key, value ->
            if (key.key.startsWith("sentinel.")) throw SpanExportRefusal("sentinel attribute on a span event")
            builder.addAttributes(keyValue(key.key, anyFromOtel(key.type, value)))
        }
        return builder.build()
    }

    private fun eventFromWireSpan(span: Span): ProducerEvent {
        if (span.endTimeUnixNano == 0L) throw SpanExportRefusal("span has not ended")
        if (span.endTimeUnixNano < span.startTimeUnixNano) throw SpanExportRefusal("span ends before it starts")
        if (span.droppedAttributesCount != 0) throw SpanExportRefusal("dropped span attributes")
        if (span.droppedLinksCount != 0) throw SpanExportRefusal("dropped links")
        if (span.droppedEventsCount != 0) throw SpanExportRefusal("dropped span events")
        val fields = readWireFields(span.attributesList)
        val linkIds = span.linksList.mapIndexed { index, link -> linkEventId(link, index) }
        val parentBytes = span.parentSpanId
        val parentPresent = parentBytes != null && parentBytes.any { it.toInt() != 0 }
        if (parentPresent && fields.parentEventId == null) {
            throw MissingPredecessorId(MALFORMED_LINK_SOURCE_PARENT, parentBytes.toStringUtf8())
        }
        val predecessors = ArrayList<String>()
        fields.parentEventId?.let { predecessors.add(it) }
        predecessors.addAll(linkIds)
        val builder =
            ProducerEvent
                .newBuilder()
                .setId(fields.eventId ?: throw SpanExportRefusal("missing $ATTRIBUTE_EVENT_ID"))
                .setSchemaVersion(fields.schemaVersion ?: throw SpanExportRefusal("missing $ATTRIBUTE_SCHEMA_VERSION"))
                .setKind(span.name)
                .setOccurrenceTime(
                    OccurrenceTime
                        .newBuilder()
                        .setClockDomainId("unix")
                        .setNanoseconds(Int128Codec.fromInt64(span.startTimeUnixNano))
                        .setUncertaintyNanoseconds(0)
                        .build(),
                ).setSequence(
                    SequenceCoordinate
                        .newBuilder()
                        .setEpoch(fields.sequenceEpoch ?: throw SpanExportRefusal("missing $ATTRIBUTE_SEQUENCE_EPOCH"))
                        .setSequence(fields.sequenceNumber ?: throw SpanExportRefusal("missing $ATTRIBUTE_SEQUENCE_NUMBER"))
                        .build(),
                ).setAcknowledgedFilterEpoch(
                    fields.filterEpoch ?: throw SpanExportRefusal("missing $ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH"),
                ).setClaimedSensitivity(
                    fields.sensitivity ?: throw SpanExportRefusal("missing $ATTRIBUTE_CLAIMED_SENSITIVITY"),
                ).addAllAttributes(fields.domain)
                .addAllCausalPredecessorIds(predecessors)
        if (fields.capabilities.isNotEmpty()) builder.addAllClaimedCapabilities(fields.capabilities)
        return builder.build()
    }

    private fun linkEventId(
        link: Span.Link,
        index: Int,
    ): String {
        if (link.droppedAttributesCount != 0) throw SpanExportRefusal("dropped link attributes at $index")
        var eventId: String? = null
        val seen = HashSet<String>()
        for (attribute in link.attributesList) {
            if (!seen.add(attribute.key)) throw SpanExportRefusal("duplicate attribute ${attribute.key}")
            if (attribute.key == ATTRIBUTE_EVENT_ID) {
                eventId = requireString(attribute)
            } else if (attribute.key.startsWith("sentinel.")) {
                throw SpanExportRefusal("unknown sentinel attribute ${attribute.key}")
            } else {
                attributeFromAny(attribute.key, attribute.value)?.let { }
                rejectNonFinite(listOf(attribute))
            }
        }
        return eventId ?: throw MissingPredecessorId(MALFORMED_LINK_SOURCE_LINK, "link-$index")
    }

    private data class WireFields(
        val eventId: String?,
        val parentEventId: String?,
        val sequenceEpoch: Long?,
        val sequenceNumber: Long?,
        val schemaVersion: String?,
        val filterEpoch: Long?,
        val sensitivity: Sensitivity?,
        val capabilities: List<SourceCapability>,
        val domain: List<AttributeEntry>,
    )

    private fun readWireFields(attributes: List<KeyValue>): WireFields {
        var eventId: String? = null
        var parentEventId: String? = null
        var sequenceEpoch: Long? = null
        var sequenceNumber: Long? = null
        var schemaVersion: String? = null
        var filterEpoch: Long? = null
        var sensitivity: Sensitivity? = null
        var capabilities: List<SourceCapability> = emptyList()
        val domain = ArrayList<AttributeEntry>()
        val seen = HashSet<String>()
        for (attribute in attributes) {
            if (!seen.add(attribute.key)) throw SpanExportRefusal("duplicate attribute ${attribute.key}")
            when (attribute.key) {
                ATTRIBUTE_EVENT_ID -> eventId = requireString(attribute)
                ATTRIBUTE_PARENT_EVENT_ID -> parentEventId = requireString(attribute)
                ATTRIBUTE_SEQUENCE_EPOCH -> sequenceEpoch = requireLong(attribute)
                ATTRIBUTE_SEQUENCE_NUMBER -> sequenceNumber = requireLong(attribute)
                ATTRIBUTE_SCHEMA_VERSION -> schemaVersion = requireString(attribute)
                ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH -> filterEpoch = requireLong(attribute)
                ATTRIBUTE_CLAIMED_SENSITIVITY -> {
                    val name = requireString(attribute)
                    val value =
                        enumByName(Sensitivity.values(), name)
                            ?: throw SpanExportRefusal("unknown claimed sensitivity $name")
                    if (value == Sensitivity.SENSITIVITY_UNSPECIFIED) {
                        throw SpanExportRefusal("claimed sensitivity must not be unspecified")
                    }
                    sensitivity = value
                }
                ATTRIBUTE_CLAIMED_CAPABILITIES -> capabilities = capabilitiesFromAny(attribute.value)
                else -> {
                    if (attribute.key.startsWith("sentinel.")) {
                        throw SpanExportRefusal("unknown sentinel attribute ${attribute.key}")
                    }
                    val mapped = attributeFromAny(attribute.key, attribute.value) ?: continue
                    domain.add(
                        AttributeEntry
                            .newBuilder()
                            .setKey(attribute.key)
                            .setValue(mapped)
                            .build(),
                    )
                }
            }
        }
        return WireFields(
            eventId,
            parentEventId,
            sequenceEpoch,
            sequenceNumber,
            schemaVersion,
            filterEpoch,
            sensitivity,
            capabilities,
            domain,
        )
    }

    private fun decodeRecord(record: LogRecord): InboundLog {
        if (record.droppedAttributesCount != 0) throw LogDecodeException("dropped log attributes")
        rejectNonFinite(record.attributesList)
        val payloadType = stringLogAttribute(record, PAYLOAD_TYPE_ATTRIBUTE) ?: throw LogDecodeException("missing $PAYLOAD_TYPE_ATTRIBUTE")
        val allowed =
            when (payloadType) {
                VERDICT_PAYLOAD_TYPE, EVENT_FILTER_PAYLOAD_TYPE -> setOf(PAYLOAD_TYPE_ATTRIBUTE)
                DECIDE_RESPONSE_PAYLOAD_TYPE -> setOf(PAYLOAD_TYPE_ATTRIBUTE, REQUEST_ID_ATTRIBUTE)
                else -> throw LogDecodeException("unsupported sentinel.payload.type $payloadType")
            }
        val seen = HashSet<String>()
        for (attribute in record.attributesList) {
            if (!seen.add(attribute.key)) throw LogDecodeException("duplicate attribute ${attribute.key}")
            if (attribute.key.startsWith("sentinel.") && attribute.key !in allowed) {
                throw LogDecodeException("unknown sentinel attribute ${attribute.key}")
            }
        }
        val body = record.body
        if (body == null || body.valueCase != AnyValue.ValueCase.BYTES_VALUE) {
            throw LogDecodeException("log body must be protobuf bytes")
        }
        val bytes = body.bytesValue.toByteArray()
        return when (payloadType) {
            VERDICT_PAYLOAD_TYPE ->
                InboundLog.VerdictLog(
                    parseMessage(bytes, "Verdict") { Verdict.parseFrom(it) },
                )
            EVENT_FILTER_PAYLOAD_TYPE ->
                InboundLog.FilterLog(
                    parseMessage(bytes, "EventFilter") { EventFilter.parseFrom(it) },
                )
            DECIDE_RESPONSE_PAYLOAD_TYPE -> InboundLog.DecisionLog(decodeDecision(record, bytes))
            else -> throw LogDecodeException("unsupported sentinel.payload.type $payloadType")
        }
    }

    private fun decodeDecision(
        record: LogRecord,
        bytes: ByteArray,
    ): DecideResponse {
        val response = parseMessage(bytes, "DecideResponse") { DecideResponse.parseFrom(it) }
        val attributeId = stringLogAttribute(record, REQUEST_ID_ATTRIBUTE)
        if (attributeId == null || attributeId != response.requestId || response.requestId.isBlank()) {
            throw LogDecodeException("sentinel.request_id must equal DecideResponse.request_id")
        }
        requireDecisionAction(response.action, response.requestId)
        for (specification in response.specificationsList) {
            requireDecisionAction(specification.action, response.requestId)
        }
        return response
    }

    private fun requireDecisionAction(
        action: DecisionAction,
        requestId: String,
    ) {
        when (action) {
            DecisionAction.DECISION_ACTION_PERMIT,
            DecisionAction.DECISION_ACTION_DENY,
            DecisionAction.DECISION_ACTION_DEFER,
            -> Unit
            DecisionAction.DECISION_ACTION_UNSPECIFIED,
            DecisionAction.UNRECOGNIZED,
            -> throw UnspecifiedDecisionAction(requestId)
        }
    }

    private fun capabilitiesFrom(attributes: Attributes): List<SourceCapability> {
        var names: List<String>? = null
        attributes.forEach { key, value ->
            if (key.key != ATTRIBUTE_CLAIMED_CAPABILITIES) return@forEach
            if (key.type != AttributeType.STRING_ARRAY) {
                throw SpanExportRefusal("claimed capabilities must be a string array")
            }
            @Suppress("UNCHECKED_CAST")
            names = value as List<String>
        }
        return names?.map { capabilityByName(it) } ?: emptyList()
    }

    private fun capabilitiesFromAny(value: AnyValue): List<SourceCapability> {
        if (value.valueCase != AnyValue.ValueCase.ARRAY_VALUE) {
            throw SpanExportRefusal("claimed capabilities must be a string array")
        }
        return value.arrayValue.valuesList.map { item ->
            if (item.valueCase != AnyValue.ValueCase.STRING_VALUE) {
                throw SpanExportRefusal("claimed capabilities must be a string array")
            }
            capabilityByName(item.stringValue)
        }
    }

    private fun capabilityByName(name: String): SourceCapability {
        val capability =
            enumByName(SourceCapability.values(), name)
                ?: throw SpanExportRefusal("unknown claimed capability $name")
        if (capability == SourceCapability.SOURCE_CAPABILITY_UNSPECIFIED) {
            throw SpanExportRefusal("claimed capability must not be unspecified")
        }
        return capability
    }

    private fun <T : Enum<T>> enumByName(
        values: Array<T>,
        name: String,
    ): T? = values.firstOrNull { it.name == name && it.name != "UNRECOGNIZED" }

    private fun refuseIncomplete(span: SpanData) {
        if (span.endEpochNanos == 0L) throw SpanExportRefusal("span has not ended")
        if (span.startEpochNanos < 0) throw SpanExportRefusal("span start is before the unix epoch")
        if (span.endEpochNanos < span.startEpochNanos) throw SpanExportRefusal("span ends before it starts")
        if (span.totalAttributeCount > span.attributes.size()) throw SpanExportRefusal("dropped span attributes")
        if (span.totalRecordedLinks > span.links.size) throw SpanExportRefusal("dropped links")
        if (span.totalRecordedEvents > span.events.size) throw SpanExportRefusal("dropped span events")
        span.links.forEachIndexed { index, link ->
            if (link.totalAttributeCount > link.attributes.size()) {
                throw SpanExportRefusal("dropped link attributes at $index")
            }
        }
        span.events.forEachIndexed { index, event ->
            if (event.totalAttributeCount > event.attributes.size()) {
                throw SpanExportRefusal("dropped span-event attributes at $index")
            }
        }
    }

    private fun outboundLog(
        payloadType: String,
        body: ByteArray,
        requestId: String?,
    ): ByteArray {
        val record =
            LogRecord
                .newBuilder()
                .setBody(bytesValue(body))
                .addAttributes(stringKv(PAYLOAD_TYPE_ATTRIBUTE, payloadType))
        if (requestId != null) record.addAttributes(stringKv(REQUEST_ID_ATTRIBUTE, requestId))
        return logsRequest(record.build())
    }

    private fun logsRequest(record: LogRecord): ByteArray =
        ExportLogsServiceRequest
            .newBuilder()
            .addResourceLogs(
                ResourceLogs
                    .newBuilder()
                    .addScopeLogs(
                        ScopeLogs.newBuilder().addLogRecords(record).build(),
                    ).build(),
            ).build()
            .toByteArray()

    private fun bytesValue(bytes: ByteArray): AnyValue = AnyValue.newBuilder().setBytesValue(ByteString.copyFrom(bytes)).build()

    private fun stringKv(
        key: String,
        value: String,
    ): KeyValue = keyValue(key, AnyValue.newBuilder().setStringValue(value).build())

    private fun intKv(
        key: String,
        value: Long,
    ): KeyValue = keyValue(key, AnyValue.newBuilder().setIntValue(value).build())

    private fun keyValue(
        key: String,
        value: AnyValue,
    ): KeyValue =
        KeyValue
            .newBuilder()
            .setKey(key)
            .setValue(value)
            .build()

    private fun anyFromAttribute(value: AttributeValue): AnyValue {
        val builder = AnyValue.newBuilder()
        when (value.valueCase) {
            AttributeValue.ValueCase.STRING_VALUE -> builder.setStringValue(value.stringValue)
            AttributeValue.ValueCase.BOOL_VALUE -> builder.setBoolValue(value.boolValue)
            AttributeValue.ValueCase.INTEGER_VALUE -> builder.setIntValue(value.integerValue)
            AttributeValue.ValueCase.DOUBLE_VALUE -> {
                if (!value.doubleValue.isFinite()) throw SpanExportRefusal("non-finite double")
                builder.setDoubleValue(value.doubleValue)
            }
            AttributeValue.ValueCase.BYTES_VALUE -> builder.setBytesValue(value.bytesValue)
            AttributeValue.ValueCase.ARRAY_VALUE -> {
                val array = ArrayValue.newBuilder()
                for (item in value.arrayValue.valuesList) array.addValues(anyFromAttribute(item))
                builder.setArrayValue(array)
            }
            AttributeValue.ValueCase.MAP_VALUE -> {
                val map = KeyValueList.newBuilder()
                for (entry in value.mapValue.entriesList) {
                    map.addValues(keyValue(entry.key, anyFromAttribute(entry.value)))
                }
                builder.setKvlistValue(map)
            }
            AttributeValue.ValueCase.VALUE_NOT_SET, null -> throw SpanExportRefusal("empty attribute value")
        }
        return builder.build()
    }

    @Suppress("UNCHECKED_CAST")
    private fun anyFromOtel(
        type: AttributeType,
        value: Any?,
    ): AnyValue {
        val builder = AnyValue.newBuilder()
        when (type) {
            AttributeType.STRING -> builder.setStringValue(value as String)
            AttributeType.BOOLEAN -> builder.setBoolValue(value as Boolean)
            AttributeType.LONG -> builder.setIntValue(value as Long)
            AttributeType.DOUBLE -> {
                val number = value as Double
                if (!number.isFinite()) throw SpanExportRefusal("non-finite double")
                builder.setDoubleValue(number)
            }
            AttributeType.STRING_ARRAY -> builder.setArrayValue(arrayOf(AttributeType.STRING, value as List<Any>))
            AttributeType.BOOLEAN_ARRAY -> builder.setArrayValue(arrayOf(AttributeType.BOOLEAN, value as List<Any>))
            AttributeType.LONG_ARRAY -> builder.setArrayValue(arrayOf(AttributeType.LONG, value as List<Any>))
            AttributeType.DOUBLE_ARRAY -> builder.setArrayValue(arrayOf(AttributeType.DOUBLE, value as List<Any>))
        }
        return builder.build()
    }

    private fun arrayOf(
        type: AttributeType,
        values: List<Any>,
    ): ArrayValue {
        val array = ArrayValue.newBuilder()
        for (item in values) array.addValues(anyFromOtel(type, item))
        return array.build()
    }

    private fun attributeFromAny(
        key: String,
        value: AnyValue?,
    ): AttributeValue? {
        if (value == null || value.valueCase == AnyValue.ValueCase.VALUE_NOT_SET) return null
        val builder = AttributeValue.newBuilder()
        when (value.valueCase) {
            AnyValue.ValueCase.STRING_VALUE -> builder.setStringValue(value.stringValue)
            AnyValue.ValueCase.BOOL_VALUE -> builder.setBoolValue(value.boolValue)
            AnyValue.ValueCase.INT_VALUE -> builder.setIntegerValue(value.intValue)
            AnyValue.ValueCase.DOUBLE_VALUE -> {
                if (!value.doubleValue.isFinite()) throw SpanExportRefusal("non-finite double $key")
                builder.setDoubleValue(value.doubleValue)
            }
            AnyValue.ValueCase.BYTES_VALUE -> builder.setBytesValue(value.bytesValue)
            AnyValue.ValueCase.ARRAY_VALUE -> {
                val array = AttributeArray.newBuilder()
                for (item in value.arrayValue.valuesList) {
                    val mapped = attributeFromAny(key, item) ?: continue
                    array.addValues(mapped)
                }
                builder.setArrayValue(array)
            }
            AnyValue.ValueCase.KVLIST_VALUE -> {
                val map = AttributeMap.newBuilder()
                for (entry in value.kvlistValue.valuesList) {
                    if (entry.key.startsWith("sentinel.")) throw SpanExportRefusal("unknown sentinel attribute ${entry.key}")
                    val mapped = attributeFromAny(entry.key, entry.value) ?: continue
                    map.addEntries(
                        AttributeEntry
                            .newBuilder()
                            .setKey(entry.key)
                            .setValue(mapped)
                            .build(),
                    )
                }
                builder.setMapValue(map)
            }
            AnyValue.ValueCase.VALUE_NOT_SET, null -> return null
        }
        return builder.build()
    }

    private fun requireString(attribute: KeyValue): String {
        val value = attribute.value ?: throw SpanExportRefusal("${attribute.key} value is empty")
        if (value.valueCase != AnyValue.ValueCase.STRING_VALUE) {
            throw SpanExportRefusal("${attribute.key} must be a string")
        }
        return value.stringValue
    }

    private fun requireLong(attribute: KeyValue): Long {
        val value = attribute.value ?: throw SpanExportRefusal("${attribute.key} value is empty")
        if (value.valueCase != AnyValue.ValueCase.INT_VALUE) {
            throw SpanExportRefusal("${attribute.key} must be an integer")
        }
        if (value.intValue < 0) throw SpanExportRefusal("${attribute.key} must be zero or positive")
        return value.intValue
    }

    private fun stringLogAttribute(
        record: LogRecord,
        key: String,
    ): String? {
        val attribute = record.attributesList.firstOrNull { it.key == key } ?: return null
        val value = attribute.value ?: return null
        if (value.valueCase != AnyValue.ValueCase.STRING_VALUE) {
            throw LogDecodeException("$key must be a string")
        }
        return value.stringValue
    }

    private fun rejectDropped(
        count: Int,
        where: String,
    ) {
        if (count != 0) throw SpanExportRefusal("dropped $where attributes")
    }

    private fun rejectNonFinite(attributes: List<KeyValue>) {
        for (attribute in attributes) {
            val value = attribute.value ?: continue
            rejectNonFiniteValue(attribute.key, value)
        }
    }

    private fun rejectNonFiniteValue(
        key: String,
        value: AnyValue,
    ) {
        when (value.valueCase) {
            AnyValue.ValueCase.DOUBLE_VALUE ->
                if (!value.doubleValue.isFinite()) throw SpanExportRefusal("non-finite double $key")
            AnyValue.ValueCase.ARRAY_VALUE ->
                value.arrayValue.valuesList.forEach { rejectNonFiniteValue(key, it) }
            AnyValue.ValueCase.KVLIST_VALUE ->
                value.kvlistValue.valuesList.forEach { entry ->
                    entry.value?.let { rejectNonFiniteValue(entry.key, it) }
                }
            else -> Unit
        }
    }

    private fun rejectWrapped(bytes: ByteArray) {
        if (bytes.size >= 2 && bytes[0] == GZIP_MAGIC_0 && bytes[1] == GZIP_MAGIC_1) {
            throw SpanExportRefusal("OTLP export is gzip compressed; the codec expects uncompressed protobuf")
        }
        var index = 0
        if (bytes.size >= 3 && bytes[0] == 0xEF.toByte() && bytes[1] == 0xBB.toByte() && bytes[2] == 0xBF.toByte()) {
            index = 3
        }
        while (index < bytes.size && bytes[index].toInt().toChar().isWhitespace()) index++
        if (index < bytes.size && (bytes[index] == '{'.code.toByte() || bytes[index] == '['.code.toByte())) {
            throw SpanExportRefusal("OTLP export is JSON; the codec expects uncompressed protobuf")
        }
    }

    private fun <T> parseMessage(
        bytes: ByteArray,
        name: String,
        parse: (ByteArray) -> T,
    ): T =
        try {
            parse(bytes)
        } catch (error: InvalidProtocolBufferException) {
            throw LogDecodeException("$name is not valid protobuf: ${error.message}", error)
        }

    private fun stringAttribute(
        attributes: Attributes,
        key: String,
    ): String? {
        var found: String? = null
        attributes.forEach { attributeKey, value ->
            if (attributeKey.key == key) {
                if (attributeKey.type != AttributeType.STRING) throw SpanExportRefusal("$key must be a string")
                found = value as String
            }
        }
        return found
    }

    private fun longAttribute(
        attributes: Attributes,
        key: String,
    ): Long? {
        var found: Long? = null
        attributes.forEach { attributeKey, value ->
            if (attributeKey.key == key) {
                if (attributeKey.type != AttributeType.LONG) throw SpanExportRefusal("$key must be an integer")
                found = value as Long
            }
        }
        return found
    }
}

private fun hexToBytes(hex: String): ByteArray {
    if (hex.length % 2 != 0) throw SpanExportRefusal("span id is not hex")
    return ByteArray(hex.length / 2) { index ->
        hex.substring(index * 2, index * 2 + 2).toInt(16).toByte()
    }
}
