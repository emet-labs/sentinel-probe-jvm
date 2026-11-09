package dev.emet.sentinel.probe.sdk.enforcement

import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EvaluationMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Readiness
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.otlp.OtlpCodec
import dev.emet.sentinel.probe.sdk.otlp.OtlpLogReceiver
import dev.emet.sentinel.probe.sdk.otlp.PROTOBUF_MEDIA_TYPE
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

// The gate's decision comes from an OTLP log. A live Connect server that would
// permit the same request must see no POST to the Decide RPC. A hit fails the test.
class GateDoesNotCallConnectTests {
    private val decidePath = "/sentinel.probe.v1.SentinelDecisionService/Decide"
    private val kind = "transfer.initiated"

    @Test
    fun `gate reads the logged DecideResponse and does not call Connect`() {
        val paths = CopyOnWriteArrayList<String>()
        val connect = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        connect.createContext("/") { exchange ->
            paths.add(exchange.requestURI.path)
            val permit =
                DecideResponse
                    .newBuilder()
                    .setRequestId("req-1")
                    .setAction(DecisionAction.DECISION_ACTION_PERMIT)
                    .build()
                    .toByteArray()
            exchange.responseHeaders.set("Content-Type", "application/proto")
            exchange.sendResponseHeaders(200, permit.size.toLong())
            exchange.responseBody.use { it.write(permit) }
        }
        connect.start()
        try {
            OtlpLogReceiver().use { receiver ->
                val logged =
                    DecideResponse
                        .newBuilder()
                        .setRequestId("req-1")
                        .setAction(DecisionAction.DECISION_ACTION_DENY)
                        .build()
                val posted =
                    HttpClient.newHttpClient().send(
                        HttpRequest
                            .newBuilder()
                            .uri(URI.create(receiver.logUrl))
                            .header("Content-Type", PROTOBUF_MEDIA_TYPE)
                            .POST(HttpRequest.BodyPublishers.ofByteArray(OtlpCodec.encodeDecision(logged)))
                            .build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                assertEquals(200, posted.statusCode())
                val client =
                    ProbeClient.fromLogReceiver(
                        ProbeClient.Config(
                            sourceHandle = "sagashop-order",
                            sentinelBaseUrl = "http://127.0.0.1:${connect.address.port}",
                        ),
                        receiver,
                    )
                val options = Options(sourceHandle = "sagashop-order", requestID = "req-1", idempotencyKey = "idem-1")
                val throughClient =
                    gate(
                        event(),
                        filter(),
                        10_000L,
                        Deps(
                            decide = client.decideFunc(),
                            nowMonotonicNs = { 0L },
                            acceptedFailModeFor = { FailMode.FAIL_MODE_OPEN },
                        ),
                        options,
                    )
                val throughReceiver =
                    receiver.enforce(
                        event(),
                        filter(),
                        10_000L,
                        { 0L },
                        { FailMode.FAIL_MODE_OPEN },
                        options,
                    )
                assertTrue(throughClient is GateOutcome.Deny)
                assertTrue(throughReceiver is GateOutcome.Deny)
                if (paths.isNotEmpty()) {
                    fail("gate called Connect; paths=$paths")
                }
                assertTrue(paths.none { it == decidePath })
            }
        } finally {
            connect.stop(0)
        }
    }

    private fun event(): ProducerEvent =
        ProducerEvent
            .newBuilder()
            .setId("evt-1")
            .setKind(kind)
            .setSchemaVersion("sentinel.model.v1")
            .build()

    private fun filter(): EventFilter =
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
                            .addEventKinds(kind)
                            .setDeliveryMode(DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK),
                    ).setFailMode(FailMode.FAIL_MODE_OPEN)
                    .setEvaluationMode(EvaluationMode.EVALUATION_MODE_ENFORCE)
                    .setReadiness(Readiness.READINESS_ACTIVE)
                    .setLatencyBudgetNanoseconds(10_000L),
            ).build()
}
