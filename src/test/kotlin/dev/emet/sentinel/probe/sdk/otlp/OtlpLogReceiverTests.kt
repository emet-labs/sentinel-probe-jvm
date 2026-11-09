package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EvaluationMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Readiness
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.model.v1.Verdict
import dev.emet.sentinel.model.v1.VerdictOutcome
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
import io.opentelemetry.proto.common.v1.AnyValue
import io.opentelemetry.proto.common.v1.KeyValue
import io.opentelemetry.proto.logs.v1.LogRecord
import io.opentelemetry.proto.logs.v1.ResourceLogs
import io.opentelemetry.proto.logs.v1.ScopeLogs
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPOutputStream
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

class OtlpLogReceiverTests {
    private val client: HttpClient = HttpClient.newHttpClient()

    @Test
    fun `gate consumes a logged decision and does not call Connect`() {
        val connects = AtomicInteger()
        val connect = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        connect.createContext("/") {
            connects.incrementAndGet()
            val body = ByteArray(0)
            it.sendResponseHeaders(200, 0)
            it.responseBody.close()
        }
        connect.start()
        try {
            OtlpLogReceiver().use { receiver ->
                val response =
                    DecideResponse
                        .newBuilder()
                        .setRequestId("req-1")
                        .setAction(DecisionAction.DECISION_ACTION_PERMIT)
                        .build()
                val posted = post(receiver, OtlpCodec.encodeDecision(response))
                assertEquals(200, posted.statusCode())
                val outcome = receiver.enforce(event(), filter(), 10_000L, { 0L }, { FailMode.FAIL_MODE_OPEN }, options())
                assertIs<GateOutcome.Permit>(outcome)
                assertEquals(0, connects.get())
            }
        } finally {
            connect.stop(0)
        }
    }

    @Test
    fun `unspecified action is an error and the gate does not permit`() {
        OtlpLogReceiver().use { receiver ->
            val response =
                DecideResponse
                    .newBuilder()
                    .setRequestId("req-unspec")
                    .setAction(DecisionAction.DECISION_ACTION_UNSPECIFIED)
                    .build()
            val posted = post(receiver, OtlpCodec.encodeDecision(response))
            assertEquals(400, posted.statusCode())
            val outcome = receiver.enforce(event(), filter(), 10_000L, { 0L }, { FailMode.FAIL_MODE_OPEN }, options("req-unspec"))
            assertTrue(outcome is GateOutcome.FailOpenPermit)
            assertTrue(outcome.reason.contains("unspecified"))
            val closed =
                receiver.enforce(
                    event(),
                    filter(FailMode.FAIL_MODE_CLOSED),
                    10_000L,
                    { 0L },
                    { FailMode.FAIL_MODE_CLOSED },
                    options("req-unspec"),
                )
            assertTrue(closed is GateOutcome.FailClosedDeny)
        }
    }

    @Test
    fun `defer still exhausts the budget`() {
        OtlpLogReceiver().use { receiver ->
            val response =
                DecideResponse
                    .newBuilder()
                    .setRequestId("req-defer")
                    .setAction(DecisionAction.DECISION_ACTION_DEFER)
                    .build()
            assertEquals(200, post(receiver, OtlpCodec.encodeDecision(response)).statusCode())
            var reads = 0
            val outcome =
                receiver.enforce(
                    event(),
                    filter(),
                    10_000L,
                    {
                        reads += 1
                        if (reads <= 2) 0L else 10_000L
                    },
                    { FailMode.FAIL_MODE_OPEN },
                    options("req-defer"),
                )
            assertTrue(outcome is GateOutcome.FailOpenPermit)
            assertEquals("defer-budget-exhausted", outcome.reason)
        }
    }

    @Test
    fun `event filter log updates the probe client`() {
        OtlpLogReceiver().use { receiver ->
            val probe =
                ProbeClient.fromLogReceiver(
                    ProbeClient.Config(sourceHandle = "sagashop-order", sentinelBaseUrl = "http://127.0.0.1:9"),
                    receiver,
                )
            val filter = EventFilter.newBuilder().setEpoch(9L).build()
            assertEquals(200, post(receiver, OtlpCodec.encodeFilter(filter)).statusCode())
            assertEquals(9L, probe.acknowledgedEpoch())
        }
    }

    @Test
    fun `verdict log is observed for identity and gzip`() {
        OtlpLogReceiver().use { receiver ->
            val verdict =
                Verdict
                    .newBuilder()
                    .setId("verdict-1")
                    .setOutcome(VerdictOutcome.VERDICT_OUTCOME_VIOLATE)
                    .build()
            val body = OtlpCodec.encodeVerdict(verdict)
            assertEquals(200, post(receiver, body).statusCode())
            assertEquals(200, post(receiver, gzip(body), "gzip").statusCode())
            assertEquals(listOf(verdict, verdict), receiver.verdicts())
        }
    }

    @Test
    fun `a non-bytes body and a wrong payload type are rejected`() {
        OtlpLogReceiver().use { receiver ->
            val textBody =
                ExportLogsServiceRequest
                    .newBuilder()
                    .addResourceLogs(
                        ResourceLogs
                            .newBuilder()
                            .addScopeLogs(
                                ScopeLogs
                                    .newBuilder()
                                    .addLogRecords(
                                        LogRecord
                                            .newBuilder()
                                            .setBody(AnyValue.newBuilder().setStringValue("nope"))
                                            .addAttributes(
                                                KeyValue
                                                    .newBuilder()
                                                    .setKey(PAYLOAD_TYPE_ATTRIBUTE)
                                                    .setValue(AnyValue.newBuilder().setStringValue(VERDICT_PAYLOAD_TYPE))
                                                    .build(),
                                            ),
                                    ),
                            ),
                    ).build()
                    .toByteArray()
            assertEquals(400, post(receiver, textBody).statusCode())
            val wrong =
                ExportLogsServiceRequest
                    .newBuilder()
                    .addResourceLogs(
                        ResourceLogs
                            .newBuilder()
                            .addScopeLogs(
                                ScopeLogs
                                    .newBuilder()
                                    .addLogRecords(
                                        LogRecord
                                            .newBuilder()
                                            .setBody(
                                                AnyValue.newBuilder().setBytesValue(
                                                    com.google.protobuf.ByteString
                                                        .copyFromUtf8("x"),
                                                ),
                                            ).addAttributes(
                                                KeyValue
                                                    .newBuilder()
                                                    .setKey(PAYLOAD_TYPE_ATTRIBUTE)
                                                    .setValue(
                                                        AnyValue
                                                            .newBuilder()
                                                            .setStringValue("sentinel.model.v1.ProducerCompletenessSignal"),
                                                    ).build(),
                                            ),
                                    ),
                            ),
                    ).build()
                    .toByteArray()
            assertEquals(400, post(receiver, wrong).statusCode())
            assertTrue(receiver.verdicts().isEmpty())
        }
    }

    private fun event(): ProducerEvent =
        ProducerEvent
            .newBuilder()
            .setId("evt-1")
            .setKind("transfer.initiated")
            .setSchemaVersion("v")
            .build()

    private fun filter(failMode: FailMode = FailMode.FAIL_MODE_OPEN): EventFilter =
        EventFilter
            .newBuilder()
            .setEpoch(5L)
            .addSpecifications(
                SpecificationFilter
                    .newBuilder()
                    .setSpecificationId("spec-1")
                    .setSpecificationVersion("1.0.0")
                    .setEventMatch(
                        EventMatch
                            .newBuilder()
                            .addEventKinds("transfer.initiated")
                            .setDeliveryMode(DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK),
                    ).setFailMode(failMode)
                    .setEvaluationMode(EvaluationMode.EVALUATION_MODE_ENFORCE)
                    .setReadiness(Readiness.READINESS_ACTIVE)
                    .setLatencyBudgetNanoseconds(10_000L),
            ).build()

    private fun options(requestId: String = "req-1"): Options =
        Options(sourceHandle = "sagashop-order", requestID = requestId, idempotencyKey = "idem-1")

    private fun post(
        receiver: OtlpLogReceiver,
        body: ByteArray,
        encoding: String? = null,
    ): HttpResponse<ByteArray> {
        val builder =
            HttpRequest
                .newBuilder()
                .uri(URI.create(receiver.logUrl))
                .header("Content-Type", PROTOBUF_MEDIA_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        if (encoding != null) builder.header("Content-Encoding", encoding)
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
    }

    private fun gzip(body: ByteArray): ByteArray {
        val output = ByteArrayOutputStream()
        GZIPOutputStream(output).use { it.write(body) }
        return output.toByteArray()
    }
}
