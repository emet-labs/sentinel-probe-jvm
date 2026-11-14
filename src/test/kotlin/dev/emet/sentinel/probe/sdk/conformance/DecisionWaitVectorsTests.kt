package dev.emet.sentinel.probe.sdk.conformance

import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EvaluationMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Readiness
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.ProbeClient
import dev.emet.sentinel.probe.sdk.config.JsonParser
import dev.emet.sentinel.probe.sdk.enforcement.DecideResult
import dev.emet.sentinel.probe.sdk.enforcement.Deps
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.sdk.enforcement.gate
import dev.emet.sentinel.probe.sdk.otlp.DecisionTimer
import dev.emet.sentinel.probe.sdk.otlp.OtlpLogReceiver
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecisionAction
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Executes the shared decision_wait suite (manifest-v1.json, 1.4.0) through the production
// OtlpLogReceiver and ProbeClient.fromLogReceiver. Only the DecisionTimer is injected: `deliver`
// POSTs the answer golden to logUrl from inside arm, and `expire` calls onExpire. No test reads
// a wall clock.
class DecisionWaitVectorsTests {
    private val root =
        Path
            .of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/probe-sdk-conformance")
    private val http: HttpClient = HttpClient.newHttpClient()

    @Suppress("UNCHECKED_CAST")
    private fun load(name: String): Map<String, Any?> {
        val parser = JsonParser(root.resolve(name).toFile().readText())
        val value = parser.parseValue() as Map<String, Any?>
        parser.skipWhitespace()
        parser.expectEof()
        return value
    }

    @Suppress("UNCHECKED_CAST")
    private fun cases(): List<Map<String, Any?>> {
        val manifest = load("manifest-v1.json")
        assertEquals("1.4.0", manifest["format_version"])
        val entry = (manifest["suites"] as List<Map<String, String>>).single { it["kind"] == "decision_wait" }
        val suite = load(entry.getValue("path"))
        assertEquals("1.2.0", suite["format_version"])
        assertEquals("decision_wait", suite["kind"])
        val cases = suite["cases"] as List<Map<String, Any?>>
        assertTrue(cases.isNotEmpty())
        return cases
    }

    private class Harness(
        val receiver: OtlpLogReceiver,
        val decide: (DecideRequest) -> DecideResult,
        val armed: List<Long>,
    )

    @Suppress("UNCHECKED_CAST")
    private fun harness(vector: Map<String, Any?>): Harness {
        val answer = Files.readAllBytes(root.resolve(vector["answer"] as String))
        val onWait = vector["on_wait"] as List<String>
        val armed = mutableListOf<Long>()
        lateinit var receiver: OtlpLogReceiver
        val timer =
            DecisionTimer { budgetNs, onExpire ->
                armed.add(budgetNs)
                for (step in onWait) {
                    when (step) {
                        "deliver" -> deliver(receiver, answer)
                        "expire" -> onExpire()
                        else -> error("unknown on_wait step $step")
                    }
                }
                AutoCloseable {}
            }
        receiver = OtlpLogReceiver(decisionTimer = timer)
        for (step in vector["before_ask"] as List<String>) {
            assertEquals("deliver", step)
            deliver(receiver, answer)
        }
        val client = ProbeClient.fromLogReceiver(ProbeClient.Config("fixture-source", receiver.logUrl), receiver)
        return Harness(receiver, client.decideFunc(), armed)
    }

    private fun deliver(
        receiver: OtlpLogReceiver,
        answer: ByteArray,
    ) {
        val request =
            HttpRequest
                .newBuilder(URI.create(receiver.logUrl))
                .header("Content-Type", "application/x-protobuf")
                .POST(HttpRequest.BodyPublishers.ofByteArray(answer))
                .build()
        assertEquals(200, http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode())
    }

    private fun assertArmed(
        vector: Map<String, Any?>,
        armed: List<Long>,
    ) {
        val want = (vector["expected"] as Map<*, *>)["armed_wait_ns"] as String?
        assertEquals(listOfNotNull(want?.toLong()), armed, vector["id"] as String)
    }

    private fun action(action: DecisionAction): String =
        when (action) {
            DecisionAction.DECISION_ACTION_PERMIT -> "permit"
            DecisionAction.DECISION_ACTION_DENY -> "deny"
            else -> "unexpected"
        }

    @Suppress("UNCHECKED_CAST")
    private fun buildFilter(
        fixtureFilter: Map<String, Any?>,
        accepted: MutableMap<String, FailMode>,
    ): EventFilter {
        val builder = EventFilter.newBuilder().setEpoch(java.lang.Long.parseUnsignedLong(fixtureFilter["epoch"] as String))
        for (fixture in fixtureFilter["specifications"] as List<Map<String, Any?>>) {
            val specId = fixture["id"] as String
            accepted[specId] = if (fixture["accepted_fail_mode"] == "closed") FailMode.FAIL_MODE_CLOSED else FailMode.FAIL_MODE_OPEN
            val delivery =
                if (fixture["delivery_mode"] == "ask_and_block") {
                    DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK
                } else {
                    DeliveryMode.DELIVERY_MODE_SHIP_ASYNC
                }
            builder.addSpecifications(
                SpecificationFilter
                    .newBuilder()
                    .setSpecificationId(specId)
                    .setFailMode(if (fixture["fail_mode"] == "closed") FailMode.FAIL_MODE_CLOSED else FailMode.FAIL_MODE_OPEN)
                    .setEvaluationMode(EvaluationMode.EVALUATION_MODE_ENFORCE)
                    .setReadiness(Readiness.READINESS_ACTIVE)
                    .setLatencyBudgetNanoseconds((fixture["latency_budget_ns"] as String).toLong())
                    .setEventMatch(
                        EventMatch
                            .newBuilder()
                            .addAllEventKinds(fixture["event_kinds"] as List<String>)
                            .setDeliveryMode(delivery),
                    ),
            )
        }
        return builder.build()
    }

    private fun kindOf(outcome: GateOutcome): String =
        when (outcome) {
            is GateOutcome.NoFilter -> "no-filter"
            is GateOutcome.Permit -> "permit"
            is GateOutcome.Deny -> "deny"
            is GateOutcome.Defer -> "defer"
            is GateOutcome.FailOpenPermit -> "fail-open-permit"
            is GateOutcome.FailClosedDeny -> "fail-closed-deny"
        }

    @Suppress("UNCHECKED_CAST")
    private fun runGateCase(vector: Map<String, Any?>) {
        val id = vector["id"] as String
        val harness = harness(vector)
        harness.receiver.use {
            val accepted = mutableMapOf<String, FailMode>()
            val filter = buildFilter(vector["filter"] as Map<String, Any?>, accepted)
            val reads = (vector["clock_reads_ns"] as List<String>).map(String::toLong)
            var readIndex = 0
            val requests = mutableListOf<DecideRequest>()
            val outcome =
                gate(
                    ProducerEvent
                        .newBuilder()
                        .setId("fixture-event")
                        .setKind((vector["event"] as Map<String, String>)["kind"])
                        .build(),
                    filter,
                    (vector["local_deadline_ns"] as String?)?.toLong(),
                    Deps(
                        decide = { request ->
                            requests.add(request)
                            harness.decide(request)
                        },
                        nowMonotonicNs = { reads.getOrElse(readIndex++) { error("$id: clock script exhausted") } },
                        acceptedFailModeFor = { spec -> accepted.getValue(spec.specificationId) },
                    ),
                    Options("fixture-source", vector["request_id"] as String, "fixture-idempotency"),
                )
            val expected = vector["expected"] as Map<String, Any?>
            assertEquals(expected["kind"], kindOf(outcome), id)
            assertEquals((expected["decide_calls"] as Long).toInt(), requests.size, id)
            assertEquals(reads.size, readIndex, id)
            assertEquals(
                (expected["remaining_transport_budget_ns"] as String).toLong(),
                requests.single().remainingTransportBudgetNanoseconds,
                id,
            )
            assertArmed(vector, harness.armed)
        }
    }

    private fun runAdapterCase(vector: Map<String, Any?>) {
        val harness = harness(vector)
        harness.receiver.use {
            val request = DecideRequest.newBuilder().setRequestId(vector["request_id"] as String)
            (vector["remaining_transport_budget_ns"] as String?)?.let {
                request.setRemainingTransportBudgetNanoseconds(java.lang.Long.parseUnsignedLong(it))
            }
            val result =
                when (val decided = harness.decide(request.build())) {
                    is DecideResult.Ok -> decided.response?.let { action(it.action) } ?: "unexpected"
                    is DecideResult.Err -> "error"
                }
            assertEquals((vector["expected"] as Map<*, *>)["result"], result, vector["id"] as String)
            assertArmed(vector, harness.armed)
        }
    }

    @Test
    fun `decision_wait gate cases run the receiver-backed adapter`() {
        val gateCases = cases().filter { it["via"] == "gate" }
        assertTrue(gateCases.isNotEmpty())
        gateCases.forEach(::runGateCase)
    }

    @Test
    fun `decision_wait adapter cases run against the receiver`() {
        val adapterCases = cases().filter { it["via"] == "adapter" }
        assertTrue(adapterCases.isNotEmpty())
        adapterCases.forEach(::runAdapterCase)
    }
}
