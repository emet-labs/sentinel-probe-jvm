package dev.emet.sentinel.probe.sdk.enforcement

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.SentinelTransport
import dev.emet.sentinel.probe.sdk.client.TransportOptions
import dev.emet.sentinel.probe.sdk.client.decideFunc
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// ConnectLoopbackTests — the JVM analog of sdk/go/enforcement/connect_loopback_test.go. A
// scripted SentinelDecisionService handler stands up on the JDK's built-in HttpServer and the
// real hand-written SentinelTransport drives through it, so the proto wire encoding, the HTTP
// transport and the Connect error mapping all get exercised end to end — with no external
// dependency and no network beyond loopback. The in-process MockDecider skips all of that, so
// these tests are the wire-level proof.
class ConnectLoopbackTests {
    private val testKind = "transfer.initiated"
    private val testOptions =
        Options(
            sourceHandle = "gateway.tool-calls",
            requestID = "req-1",
            idempotencyKey = "idem-1",
        )

    private fun askAndBlockSpec(): SpecificationFilter =
        SpecificationFilter
            .newBuilder()
            .setSpecificationId("spec-1")
            .setSpecificationVersion("1.0.0")
            .setEventMatch(
                EventMatch
                    .newBuilder()
                    .addEventKinds(testKind)
                    .setDeliveryMode(DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK)
                    .build(),
            ).setFailMode(FailMode.FAIL_MODE_OPEN)
            .build()

    private fun closedAskAndBlockSpec(): SpecificationFilter = askAndBlockSpec().toBuilder().setFailMode(FailMode.FAIL_MODE_CLOSED).build()

    private fun makeFilter(
        epoch: Long,
        spec: SpecificationFilter,
    ): EventFilter =
        EventFilter
            .newBuilder()
            .setEpoch(epoch)
            .addSpecifications(spec)
            .build()

    private fun makeEvent(): ProducerEvent =
        ProducerEvent
            .newBuilder()
            .setId("evt-1")
            .setKind(testKind)
            .setSchemaVersion("sentinel.model.v1")
            .build()

    private fun startLoopback(handler: (HttpExchange) -> Unit): HttpServer {
        val server = HttpServer.create(InetSocketAddress(0), 0)
        server.createContext("/") { handler(it) }
        server.start()
        return server
    }

    private fun sendProto(
        exchange: HttpExchange,
        response: DecideResponse,
    ) {
        val bytes = response.toByteArray()
        exchange.responseHeaders.set("Content-Type", "application/proto")
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.write(bytes)
        exchange.responseBody.close()
    }

    private fun sendConnectError(
        exchange: HttpExchange,
        code: String,
        message: String,
    ) {
        val body = """{"code":"$code","message":"$message"}""".toByteArray(Charsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(503, body.size.toLong())
        exchange.responseBody.write(body)
        exchange.responseBody.close()
    }

    private fun transportFor(server: HttpServer) =
        SentinelTransport(
            TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}"),
        )

    private fun permitResponse() = DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()

    private fun denyResponse() = DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_DENY).build()

    @Test
    fun `deny over the real wire`() {
        val server = startLoopback { ex -> sendProto(ex, denyResponse()) }
        try {
            val deps =
                Deps(
                    decide = decideFunc(transportFor(server)),
                    nowMonotonicNs = { 0L },
                    acceptedFailModeFor = { FailMode.FAIL_MODE_OPEN },
                )
            val outcome = gate(makeEvent(), makeFilter(5L, askAndBlockSpec()), 10000L, deps, testOptions)
            assertTrue(outcome is GateOutcome.Deny)
            assertEquals(5L, outcome.filterEpoch)
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `permit over the real wire`() {
        val server = startLoopback { ex -> sendProto(ex, permitResponse()) }
        try {
            val deps =
                Deps(decide = decideFunc(transportFor(server)), nowMonotonicNs = { 0L }, acceptedFailModeFor = { FailMode.FAIL_MODE_OPEN })
            val outcome = gate(makeEvent(), makeFilter(5L, askAndBlockSpec()), 10000L, deps, testOptions)
            assertTrue(outcome is GateOutcome.Permit)
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `server error fails closed when contracted`() {
        // The handler returns a coded Connect error; the client surfaces a ConnectError; the gate
        // routes it into the contracted fail mode with the code recorded for audit.
        val server = startLoopback { ex -> sendConnectError(ex, "unavailable", "endpoint down") }
        try {
            val deps =
                Deps(
                    decide = decideFunc(transportFor(server)),
                    nowMonotonicNs = { 0L },
                    acceptedFailModeFor = { FailMode.FAIL_MODE_CLOSED },
                )
            val outcome = gate(makeEvent(), makeFilter(5L, closedAskAndBlockSpec()), 10000L, deps, testOptions)
            assertTrue(outcome is GateOutcome.FailClosedDeny)
            assertTrue(outcome.reason.startsWith("connect-unavailable"), "reason = ${outcome.reason}")
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `unreachable endpoint fails open`() {
        // Nothing is listening, so the client's own transport error reaches the gate and routes
        // into the default fail-open mode.
        val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:1"))
        val deps = Deps(decide = decideFunc(transport), nowMonotonicNs = { 0L }, acceptedFailModeFor = { FailMode.FAIL_MODE_OPEN })
        val outcome = gate(makeEvent(), makeFilter(5L, askAndBlockSpec()), 10000L, deps, testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertTrue(outcome.reason.startsWith("transport-error"), "reason = ${outcome.reason}")
    }
}
