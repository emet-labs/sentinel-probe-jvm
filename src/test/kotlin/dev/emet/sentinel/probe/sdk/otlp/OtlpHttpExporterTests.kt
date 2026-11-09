package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.Int128
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SourceActivityState
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
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.sdk.trace.data.StatusData
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OtlpHttpExporterTests {
    @Test
    fun `trace and completeness posts use protobuf and the source handle header`() {
        val captured = AtomicReference<HttpExchange?>(null)
        val bodies = ArrayList<ByteArray>()
        val paths = ArrayList<String>()
        val handles = ArrayList<String>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            val body = exchange.requestBody.readBytes()
            synchronized(bodies) {
                bodies.add(body)
                paths.add(exchange.requestURI.path)
                handles.add(exchange.requestHeaders.getFirst(SOURCE_HANDLE_HEADER) ?: "")
                assertEquals(PROTOBUF_MEDIA_TYPE, exchange.requestHeaders.getFirst("Content-Type"))
                assertTrue(exchange.requestHeaders.getFirst("Content-Encoding") == null)
            }
            captured.set(exchange)
            exchange.sendResponseHeaders(200, 0)
            exchange.responseBody.close()
        }
        server.start()
        try {
            val exporter = OtlpHttpExporter("http://127.0.0.1:${server.address.port}", "sagashop-payment")
            exporter.exportSpan(chargeSpan())
            exporter.exportCompleteness(signal())
            assertEquals(listOf(TRACES_PATH, LOGS_PATH), paths)
            assertEquals(listOf("sagashop-payment", "sagashop-payment"), handles)
            val trace = ExportTraceServiceRequest.parseFrom(bodies[0])
            val span =
                trace.resourceSpansList
                    .single()
                    .scopeSpansList
                    .single()
                    .spansList
                    .single()
            assertEquals(
                "01f0c0ffee-charge",
                span.attributesList
                    .first { it.key == ATTRIBUTE_EVENT_ID }
                    .value.stringValue,
            )
            assertEquals("sagashop-payment", handles[0])
            val logs = ExportLogsServiceRequest.parseFrom(bodies[1])
            val record =
                logs.resourceLogsList
                    .single()
                    .scopeLogsList
                    .single()
                    .logRecordsList
                    .single()
            val type =
                record.attributesList
                    .first { it.key == PAYLOAD_TYPE_ATTRIBUTE }
                    .value.stringValue
            assertEquals(COMPLETENESS_PAYLOAD_TYPE, type)
            assertTrue(record.body.valueCase == io.opentelemetry.proto.common.v1.AnyValue.ValueCase.BYTES_VALUE)
        } finally {
            server.stop(0)
        }
    }

    private fun chargeSpan() =
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
                    .put(AttributeKey.stringKey("sagashop.payment.status"), "success")
                    .build(),
            ).setStatus(StatusData.unset())
            .setHasEnded(true)
            .setTotalRecordedEvents(0)
            .setTotalRecordedLinks(0)
            .setTotalAttributeCount(7)
            .setResource(Resource.getDefault())
            .setInstrumentationScopeInfo(InstrumentationScopeInfo.empty())
            .build()

    private fun signal(): ProducerCompletenessSignal {
        val whenNanos =
            OccurrenceTime
                .newBuilder()
                .setClockDomainId("unix")
                .setNanoseconds(
                    Int128
                        .newBuilder()
                        .setHigh(0)
                        .setLow(1_700_000_000_000_000_000L)
                        .build(),
                ).setUncertaintyNanoseconds(0)
                .build()
        return ProducerCompletenessSignal
            .newBuilder()
            .setSequenceEpoch(7L)
            .setLastEmitted(1L)
            .setCompleteThrough(1L)
            .setProducerReportedAt(whenNanos)
            .setAcknowledgedFilterEpoch(1L)
            .setActivity(SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE)
            .setActivitySince(whenNanos)
            .build()
    }
}
