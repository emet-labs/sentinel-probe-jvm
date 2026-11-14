package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EvaluationMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Readiness
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.enforcement.DecideResult
import dev.emet.sentinel.probe.sdk.enforcement.Deps
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.sdk.enforcement.gate
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Path
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

// The decide adapter waits for a DecideResponse log within the request's remaining transport
// budget. Every test drives the wait through an injected DecisionTimer: nothing sleeps and no
// test reads a wall clock.
class DecisionWaitTests {
    private val http: HttpClient = HttpClient.newHttpClient()

    // ScriptedTimer records each armed budget and runs onArm, which may deliver a log or expire
    // the wait, from inside arm.
    private class ScriptedTimer : DecisionTimer {
        val armed = ArrayList<Long>()
        var disarmed = 0
        var onArm: (expire: () -> Unit) -> Unit = {}

        override fun arm(
            budgetNs: Long,
            onExpire: () -> Unit,
        ): AutoCloseable {
            armed.add(budgetNs)
            onArm(onExpire)
            return AutoCloseable { disarmed += 1 }
        }
    }

    @Test
    fun `an answer that arrives after the ask is returned`() {
        val timer = ScriptedTimer()
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            timer.onArm = { post(receiver, decision("req-late", DecisionAction.DECISION_ACTION_DENY), 200) }
            val result = receiver.decide(request("req-late", 40_000_000L))
            assertIs<DecideResult.Ok>(result)
            assertEquals(DecisionAction.DECISION_ACTION_DENY, result.response?.action)
            assertEquals(listOf(40_000_000L), timer.armed)
            assertEquals(1, timer.disarmed)
        }
    }

    @Test
    fun `a stored answer wins without arming a wait`() {
        val timer = ScriptedTimer()
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            post(receiver, decision("req-early", DecisionAction.DECISION_ACTION_PERMIT), 200)
            assertIs<DecideResult.Ok>(receiver.decide(request("req-early", null)))
            assertIs<DecideResult.Ok>(receiver.decide(request("req-early", 40_000_000L)))
            assertTrue(timer.armed.isEmpty())
        }
    }

    @Test
    fun `an absent or zero budget does not wait`() {
        val timer = ScriptedTimer()
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            assertIs<DecideResult.Err>(receiver.decide(request("req-none", null)))
            assertIs<DecideResult.Err>(receiver.decide(request("req-zero", 0L)))
            assertTrue(timer.armed.isEmpty())
        }
    }

    @Test
    fun `expiry fails the ask and a late answer does not change it`() {
        val timer = ScriptedTimer()
        timer.onArm = { expire -> expire() }
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            val result = receiver.decide(request("req-expired", 40_000_000L))
            assertIs<DecideResult.Err>(result)
            assertIs<TimeoutException>(result.error)
            post(receiver, decision("req-expired", DecisionAction.DECISION_ACTION_PERMIT), 200)
        }
    }

    @Test
    fun `an answer for another request does not answer the ask`() {
        val timer = ScriptedTimer()
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            timer.onArm = { expire ->
                post(receiver, decision("req-other", DecisionAction.DECISION_ACTION_DENY), 200)
                expire()
            }
            val result = receiver.decide(request("req-mine", 40_000_000L))
            assertIs<DecideResult.Err>(result)
            assertIs<TimeoutException>(result.error)
        }
    }

    @Test
    fun `an unspecified action for the id fails the wait at once and a valid answer clears it`() {
        val timer = ScriptedTimer()
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            timer.onArm = { post(receiver, decision("req-bad", DecisionAction.DECISION_ACTION_UNSPECIFIED), 400) }
            val result = receiver.decide(request("req-bad", 40_000_000L))
            assertIs<DecideResult.Err>(result)
            assertIs<UnspecifiedDecisionAction>(result.error)
            timer.onArm = {}
            post(receiver, decision("req-bad", DecisionAction.DECISION_ACTION_DENY), 200)
            val cleared = receiver.decide(request("req-bad", null))
            assertIs<DecideResult.Ok>(cleared)
            assertEquals(DecisionAction.DECISION_ACTION_DENY, cleared.response?.action)
        }
    }

    @Test
    fun `close fails a pending wait and later asks`() {
        val timer = ScriptedTimer()
        val receiver = OtlpLogReceiver(decisionTimer = timer)
        timer.onArm = { receiver.close() }
        val result = receiver.decide(request("req-closed", 40_000_000L))
        assertIs<DecideResult.Err>(result)
        assertIs<IllegalStateException>(result.error)
        assertIs<DecideResult.Err>(receiver.decide(request("req-after", 40_000_000L)))
        assertEquals(1, timer.armed.size)
    }

    @Test
    fun `a budget of 2^63 ns or more is capped at Long MAX_VALUE`() {
        val timer = ScriptedTimer()
        timer.onArm = { expire -> expire() }
        OtlpLogReceiver(decisionTimer = timer).use { receiver ->
            assertIs<DecideResult.Err>(receiver.decide(request("req-huge", -1L)))
            assertEquals(listOf(Long.MAX_VALUE), timer.armed)
        }
    }

    @Test
    fun `the default timer expires a short budget`() {
        // The default scheduler path, with a budget of 1 ns so the ask fails at once.
        OtlpLogReceiver().use { receiver ->
            val result = receiver.decide(request("req-default", 1L))
            assertIs<DecideResult.Err>(result)
            assertIs<TimeoutException>(result.error)
        }
    }

    // The production adapter, ProbeClient.fromLogReceiver(...).decideFunc(), through the gate.
    // The open spec has a 50 ms budget; the clock reads 1 ms then 11 ms, so 40 ms remain.
    @Test
    fun `the gate waits for the decision within the remaining budget`() {
        data class Case(
            val failMode: FailMode,
            val deliver: Boolean,
            val expect: (GateOutcome) -> Boolean,
        )
        val cases =
            listOf(
                Case(FailMode.FAIL_MODE_OPEN, true) { it is GateOutcome.Deny },
                Case(FailMode.FAIL_MODE_OPEN, false) { it is GateOutcome.FailOpenPermit },
                Case(FailMode.FAIL_MODE_CLOSED, false) { it is GateOutcome.FailClosedDeny },
            )
        for (case in cases) {
            val timer = ScriptedTimer()
            OtlpLogReceiver(decisionTimer = timer).use { receiver ->
                timer.onArm = { expire ->
                    if (case.deliver) post(receiver, golden("decide-response.bin"), 200) else expire()
                }
                val probe =
                    ProbeClient.fromLogReceiver(
                        ProbeClient.Config(sourceHandle = "sagashop-order", sentinelBaseUrl = "http://127.0.0.1:9"),
                        receiver,
                    )
                val reads = ArrayDeque(listOf(1_000_000L, 11_000_000L))
                val outcome =
                    gate(
                        event(),
                        filter(case.failMode),
                        null,
                        Deps(
                            decide = probe.decideFunc(),
                            nowMonotonicNs = { reads.removeFirst() },
                            acceptedFailModeFor = { it.failMode },
                        ),
                        Options(sourceHandle = "sagashop-order", requestID = "req-charge-1", idempotencyKey = "idem-1"),
                    )
                assertTrue(case.expect(outcome), "outcome $outcome for $case")
                assertTrue(reads.isEmpty(), "every clock read is consumed")
                assertEquals(listOf(40_000_000L), timer.armed)
            }
        }
    }

    private fun request(
        requestId: String,
        budgetNs: Long?,
    ): DecideRequest {
        val builder = DecideRequest.newBuilder().setRequestId(requestId)
        if (budgetNs != null) builder.setRemainingTransportBudgetNanoseconds(budgetNs)
        return builder.build()
    }

    private fun decision(
        requestId: String,
        action: DecisionAction,
    ): ByteArray =
        OtlpCodec.encodeDecision(
            DecideResponse
                .newBuilder()
                .setRequestId(requestId)
                .setAction(action)
                .build(),
        )

    private fun golden(name: String): ByteArray =
        Path
            .of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/otlp")
            .resolve(name)
            .toFile()
            .readBytes()

    private fun event(): ProducerEvent =
        ProducerEvent
            .newBuilder()
            .setId("evt-1")
            .setKind("transfer.initiated")
            .setSchemaVersion("v")
            .build()

    private fun filter(failMode: FailMode): EventFilter =
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
                    .setLatencyBudgetNanoseconds(50_000_000L),
            ).build()

    private fun post(
        receiver: OtlpLogReceiver,
        body: ByteArray,
        status: Int,
    ) {
        val response =
            http.send(
                HttpRequest
                    .newBuilder()
                    .uri(URI.create(receiver.logUrl))
                    .header("Content-Type", PROTOBUF_MEDIA_TYPE)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build(),
                HttpResponse.BodyHandlers.ofByteArray(),
            )
        assertEquals(status, response.statusCode())
    }
}
