package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.Verdict
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.v1.DecideResponse
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.common.v1.KeyValue
import io.opentelemetry.proto.logs.v1.LogRecord
import io.opentelemetry.proto.trace.v1.Span
import org.junit.jupiter.api.Test
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OtlpGoldenTests {
    private val root =
        Path
            .of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/otlp")

    @Test
    fun `charge-ship trace golden carries the contract attributes`() {
        val request = ExportTraceServiceRequest.parseFrom(root.resolve("charge-ship.bin").toFile().readBytes())
        val spans =
            request.resourceSpansList.flatMap { resource ->
                resource.scopeSpansList.flatMap { it.spansList }
            }
        assertEquals(2, spans.size)
        val charge = spans[0]
        val ship = spans[1]
        assertEquals("sagashop.payment.charge.completed", charge.name)
        assertEquals("sagashop.order.ship.requested", ship.name)
        assertEquals("01f0c0ffee-charge", stringAttr(charge, ATTRIBUTE_EVENT_ID))
        assertEquals("01f0c0ffee-ship", stringAttr(ship, ATTRIBUTE_EVENT_ID))
        assertNull(stringAttr(charge, ATTRIBUTE_PARENT_EVENT_ID))
        assertEquals("01f0c0ffee-charge", stringAttr(ship, ATTRIBUTE_PARENT_EVENT_ID))
        assertEquals("sagashop-contract-v1", stringAttr(charge, ATTRIBUTE_SCHEMA_VERSION))
        assertEquals("sagashop-contract-v1", stringAttr(ship, ATTRIBUTE_SCHEMA_VERSION))
        assertEquals(7L, longAttr(charge, ATTRIBUTE_SEQUENCE_EPOCH))
        assertEquals(1L, longAttr(charge, ATTRIBUTE_SEQUENCE_NUMBER))
        assertEquals(2L, longAttr(ship, ATTRIBUTE_SEQUENCE_NUMBER))
        assertEquals(4L, longAttr(charge, ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH))
        assertEquals(4L, longAttr(ship, ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH))
        assertEquals("SENSITIVITY_INTERNAL", stringAttr(charge, ATTRIBUTE_CLAIMED_SENSITIVITY))
        assertEquals("success", stringAttr(charge, "sagashop.payment.status"))
        assertEquals("order-1001", stringAttr(charge, "sagashop.order.id"))
        assertEquals("order-1001", stringAttr(ship, "sagashop.order.id"))
        val events = OtlpCodec.decodeTraceEvents(root.resolve("charge-ship.bin").toFile().readBytes())
        assertEquals(listOf("01f0c0ffee-charge", "01f0c0ffee-ship"), events.map { it.id })
        assertEquals(emptyList(), events[0].causalPredecessorIdsList)
        assertEquals(listOf("01f0c0ffee-charge"), events[1].causalPredecessorIdsList)
    }

    @Test
    fun `completeness golden is one active signal`() {
        val record = onlyRecord(root.resolve("completeness-order.bin").toFile().readBytes())
        assertEquals(COMPLETENESS_PAYLOAD_TYPE, stringAttr(record, PAYLOAD_TYPE_ATTRIBUTE))
        assertEquals("sagashop-order", stringAttr(record, SOURCE_HANDLE_ATTRIBUTE))
        assertNull(stringAttr(record, REQUEST_ID_ATTRIBUTE))
        val signal = ProducerCompletenessSignal.parseFrom(record.body.bytesValue)
        assertEquals(7L, signal.sequenceEpoch)
        assertEquals(2L, signal.lastEmitted)
        assertEquals(2L, signal.completeThrough)
        assertEquals(4L, signal.acknowledgedFilterEpoch)
        assertEquals("unix", signal.producerReportedAt.clockDomainId)
        assertEquals(0L, signal.producerReportedAt.uncertaintyNanoseconds)
        assertEquals(1_700_000_000_000_000_000L, signal.producerReportedAt.nanoseconds.low)
    }

    @Test
    fun `return-path goldens decode and completeness does not`() {
        OtlpLogReceiver().use { receiver ->
            val filters = ArrayList<EventFilter>()
            receiver.addFilterListener { filters.add(it) }
            post(receiver, root.resolve("verdict.bin").toFile().readBytes(), 200)
            post(receiver, root.resolve("event-filter.bin").toFile().readBytes(), 200)
            post(receiver, root.resolve("decide-response.bin").toFile().readBytes(), 200)
            post(receiver, root.resolve("completeness-order.bin").toFile().readBytes(), 400)
            val verdict = Verdict.parseFrom(onlyRecord(root.resolve("verdict.bin").toFile().readBytes()).body.bytesValue)
            assertEquals(listOf(verdict), receiver.verdicts())
            val filter = EventFilter.parseFrom(onlyRecord(root.resolve("event-filter.bin").toFile().readBytes()).body.bytesValue)
            assertEquals(listOf(filter), filters)
            val decision = DecideResponse.parseFrom(onlyRecord(root.resolve("decide-response.bin").toFile().readBytes()).body.bytesValue)
            assertEquals(
                decision.requestId,
                stringAttr(onlyRecord(root.resolve("decide-response.bin").toFile().readBytes()), REQUEST_ID_ATTRIBUTE),
            )
            val result =
                receiver.decide(
                    dev.emet.sentinel.probe.v1.DecideRequest
                        .newBuilder()
                        .setRequestId(decision.requestId)
                        .build(),
                )
            assertTrue(result is dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Ok)
            assertEquals(decision, result.response)
        }
    }

    private fun post(
        receiver: OtlpLogReceiver,
        body: ByteArray,
        status: Int,
    ) {
        val client =
            java.net.http.HttpClient
                .newHttpClient()
        val request =
            java.net.http.HttpRequest
                .newBuilder()
                .uri(java.net.URI.create(receiver.logUrl))
                .header("Content-Type", PROTOBUF_MEDIA_TYPE)
                .POST(
                    java.net.http.HttpRequest.BodyPublishers
                        .ofByteArray(body),
                ).build()
        val response =
            client.send(
                request,
                java.net.http.HttpResponse.BodyHandlers
                    .ofByteArray(),
            )
        assertEquals(status, response.statusCode(), response.body().toString(Charsets.UTF_8))
    }

    private fun onlyRecord(bytes: ByteArray): LogRecord {
        val request = ExportLogsServiceRequest.parseFrom(bytes)
        val records =
            request.resourceLogsList.flatMap { resource ->
                resource.scopeLogsList.flatMap { it.logRecordsList }
            }
        assertEquals(1, records.size)
        return records[0]
    }

    private fun stringAttr(
        span: Span,
        key: String,
    ): String? = stringValue(span.attributesList, key)

    private fun longAttr(
        span: Span,
        key: String,
    ): Long? {
        val value = span.attributesList.firstOrNull { it.key == key }?.value ?: return null
        return if (value.valueCase == AnyValue.ValueCase.INT_VALUE) value.intValue else null
    }

    private fun stringAttr(
        record: LogRecord,
        key: String,
    ): String? = stringValue(record.attributesList, key)

    private fun stringValue(
        attributes: List<KeyValue>,
        key: String,
    ): String? {
        val value = attributes.firstOrNull { it.key == key }?.value ?: return null
        return if (value.valueCase == AnyValue.ValueCase.STRING_VALUE) value.stringValue else null
    }
}
