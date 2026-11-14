package dev.emet.sentinel.probe.sdk.conformance

import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EvaluationMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.Int128
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.Readiness
import dev.emet.sentinel.model.v1.SourceActivityState
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.model.v1.VerdictOutcome
import dev.emet.sentinel.probe.sdk.config.JsonParser
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import dev.emet.sentinel.probe.sdk.enforcement.DecideResult
import dev.emet.sentinel.probe.sdk.enforcement.Deps
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.sdk.enforcement.gate
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import dev.emet.sentinel.probe.sdk.internal.specmatch.SpecMatch
import dev.emet.sentinel.probe.sdk.otlp.InboundLog
import dev.emet.sentinel.probe.sdk.otlp.OtlpCodec
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest
import io.opentelemetry.proto.logs.v1.LogRecord
import java.math.BigInteger
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConformanceVectorsTests {
    private val root =
        Path
            .of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/probe-sdk-conformance")

    private fun load(name: String): Map<String, Any?> = loadVersion(name, "1.0.0")

    @Suppress("UNCHECKED_CAST")
    private fun loadVersion(
        name: String,
        version: String,
    ): Map<String, Any?> {
        val value = loadUnchecked(name)
        assertEquals(version, value["format_version"])
        return value
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadUnchecked(name: String): Map<String, Any?> {
        val parser = JsonParser(root.resolve(name).toFile().readText())
        val value = parser.parseValue() as Map<String, Any?>
        parser.skipWhitespace()
        parser.expectEof()
        return value
    }

    @Test
    fun `manifest suite registry fails closed`() {
        val manifest = loadVersion("manifest-v1.json", "1.3.0")
        val suites = manifest["suites"] as List<Map<String, String>>
        assertEquals(
            listOf("spec_match", "int128", "enforcement_gate", "otlp_signal", "sequence_completeness"),
            suites.map { it.getValue("kind") },
        )
    }

    @Test
    fun `shared malformed corpus is rejected by category`() {
        val manifest = loadVersion("manifest-v1.json", "1.3.0")
        val fixtures = manifest["malformed"] as List<Map<String, String>>
        for (fixture in fixtures) {
            assertEquals(
                fixture.getValue("rejection_category"),
                malformedCategory(fixture.getValue("path")),
                fixture.getValue("path"),
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun malformedCategory(name: String): String {
        val parser = JsonParser(root.resolve(name).toFile().readText())
        val value =
            try {
                (parser.parseValue() as Map<String, Any?>).also {
                    parser.skipWhitespace()
                    parser.expectEof()
                }
            } catch (error: IllegalArgumentException) {
                return if (error.message?.contains("duplicate JSON key") == true) "duplicate-key" else "syntax"
            }
        if (value["format_version"] != "1.0.0") return "version"
        if (!value.containsKey("cases")) return "missing-field"
        if (value.keys.any { it !in setOf("format_version", "kind", "cases") }) return "unknown-field"
        val kind = value["kind"] as? String
        if (kind !in setOf("spec_match", "int128", "enforcement_gate")) return "unknown-token"
        if (kind == "int128") {
            val cases = value["cases"] as List<Map<String, String>>
            if (cases.map { it["id"] }.toSet().size != cases.size) return "duplicate-id"
            val decimal = Regex("^(0|-?[1-9][0-9]*)$")
            if (
                cases.any { case ->
                    listOf("value", "high", "low").any {
                        decimal.matchEntire(case[it].orEmpty()) == null
                    }
                }
            ) {
                return "integer-lexeme"
            }
            val minimum = BigInteger.ONE.shiftLeft(127).negate()
            val maximum = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE)
            if (cases.any { BigInteger(it.getValue("value")) !in minimum..maximum }) return "integer-range"
        }
        return "accepted"
    }

    @Test
    fun `exact Int128 words and independent decoding follow shared vectors`() {
        val suite = load("int128-v1.json")
        assertEquals("int128", suite["kind"])
        val cases = suite["cases"] as List<Map<String, String>>
        assertEquals(cases.size, cases.map { it.getValue("id") }.toSet().size)
        for (vector in cases) {
            val value = BigInteger(vector.getValue("value"))
            val high = vector.getValue("high").toLong()
            val low = BigInteger(vector.getValue("low"))
            assertTrue(low.signum() >= 0 && low.bitLength() <= 64)
            val encoded = Int128Codec.fromBigInt(value)
            assertEquals(high, encoded.high, "${vector["id"]} high")
            assertEquals(low.toLong(), encoded.low, "${vector["id"]} low bits")
            val words =
                Int128
                    .newBuilder()
                    .setHigh(high)
                    .setLow(low.toLong())
                    .build()
            assertEquals(value, Int128Codec.toBigInt(words), "${vector["id"]} decode")
        }
    }

    @Test
    fun `SpecMatch follows shared vectors`() {
        val suite = load("spec-match-v1.json")
        assertEquals("spec_match", suite["kind"])
        val cases = suite["cases"] as List<Map<String, Any?>>
        assertEquals(cases.size, cases.map { it["id"] }.toSet().size)
        for (vector in cases) {
            val filter = vector["specification_filter"] as Map<String, Any?>
            val match = filter["event_match"] as Map<String, Any?>?
            val builder = SpecificationFilter.newBuilder()
            if (match != null) {
                val matchBuilder =
                    dev.emet.sentinel.model.v1.EventMatch
                        .newBuilder()
                matchBuilder.addAllEventKinds(match["event_kinds"] as List<String>)
                matchBuilder.addAllProjectedAttributeKeys(match["projected_attribute_keys"] as List<String>)
                builder.eventMatch = matchBuilder.build()
            }
            val eventMap = vector["producer_event"] as Map<String, Any?>
            val event = ProducerEvent.newBuilder().setKind(eventMap["kind"] as String).build()
            assertEquals(vector["expected"], SpecMatch.selects(builder.build(), event), vector["id"] as String)
        }
    }

    @Test
    @Suppress("NestedBlockDepth")
    fun `enforcement scenarios execute the production gate`() {
        val suite = load("enforcement-gate-v1.json")
        val cases = suite["cases"] as List<Map<String, Any?>>
        for (vector in cases) {
            val accepted = mutableMapOf<String, FailMode>()
            val fixtureFilter = vector["filter"] as Map<String, Any?>?
            val filter =
                fixtureFilter?.let {
                    val builder = EventFilter.newBuilder().setEpoch((it["epoch"] as String).toLong())
                    for (fixture in it["specifications"] as List<Map<String, Any?>>) {
                        val id = fixture["id"] as String
                        accepted[id] = if (fixture["accepted_fail_mode"] == "closed") FailMode.FAIL_MODE_CLOSED else FailMode.FAIL_MODE_OPEN
                        builder.addSpecifications(
                            SpecificationFilter
                                .newBuilder()
                                .setSpecificationId(id)
                                .setFailMode(if (fixture["fail_mode"] == "closed") FailMode.FAIL_MODE_CLOSED else FailMode.FAIL_MODE_OPEN)
                                .setEvaluationMode(EvaluationMode.EVALUATION_MODE_ENFORCE)
                                .setReadiness(Readiness.READINESS_ACTIVE)
                                .setLatencyBudgetNanoseconds((fixture["latency_budget_ns"] as String).toLong())
                                .setEventMatch(
                                    dev.emet.sentinel.model.v1.EventMatch
                                        .newBuilder()
                                        .addAllEventKinds(fixture["event_kinds"] as List<String>)
                                        .setDeliveryMode(
                                            if (fixture["delivery_mode"] ==
                                                "ask_and_block"
                                            ) {
                                                DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK
                                            } else {
                                                DeliveryMode.DELIVERY_MODE_SHIP_ASYNC
                                            },
                                        ),
                                ),
                        )
                    }
                    builder.build()
                }
            val reads = (vector["clock_reads_ns"] as List<String>).map(String::toLong)
            var readIndex = 0
            val requests = mutableListOf<DecideRequest>()
            val decider = (vector["decider"] as Map<String, String>)["result"]!!
            val actions =
                mapOf(
                    "permit" to DecisionAction.DECISION_ACTION_PERMIT,
                    "deny" to DecisionAction.DECISION_ACTION_DENY,
                    "defer" to DecisionAction.DECISION_ACTION_DEFER,
                    "unspecified" to DecisionAction.DECISION_ACTION_UNSPECIFIED,
                )
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
                            if (decider ==
                                "transport_error"
                            ) {
                                DecideResult.Err(IllegalStateException("fixture-transport-error"))
                            } else {
                                DecideResult.Ok(DecideResponse.newBuilder().setAction(actions.getValue(decider)).build())
                            }
                        },
                        nowMonotonicNs = { reads.getOrElse(readIndex++) { error("clock script exhausted") } },
                        acceptedFailModeFor = { spec -> accepted.getValue(spec.specificationId) },
                    ),
                    Options("fixture-source", "fixture-request", "fixture-idempotency"),
                )
            val kind =
                when (outcome) {
                    is GateOutcome.NoFilter -> "no-filter"
                    is GateOutcome.Permit -> "permit"
                    is GateOutcome.Deny -> "deny"
                    is GateOutcome.Defer -> "defer"
                    is GateOutcome.FailOpenPermit -> "fail-open-permit"
                    is GateOutcome.FailClosedDeny -> "fail-closed-deny"
                }
            val expected = vector["expected"] as Map<String, Any?>
            assertEquals(expected["kind"], kind, vector["id"] as String)
            assertEquals((expected["decide_calls"] as Long).toInt(), requests.size)
            assertEquals(reads.size, readIndex)
        }
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `otlp_signal vectors decode events and messages`() {
        val suite = loadVersion("otlp-signal-v1.json", "1.1.0")
        assertEquals("otlp_signal", suite["kind"])
        val cases = suite["cases"] as List<Map<String, Any?>>
        assertEquals(cases.size, cases.map { it["id"] }.toSet().size)
        for (vector in cases) {
            val bytes = root.resolve(vector.getValue("path") as String).toFile().readBytes()
            when (vector.getValue("signal")) {
                "trace" -> assertTrace(vector, bytes)
                "completeness" -> assertCompleteness(vector, bytes)
                "verdict" -> assertVerdict(vector, bytes)
                "event_filter" -> assertFilter(vector, bytes)
                "decide_response" -> assertDecision(vector, bytes)
                else -> error(vector["signal"].toString())
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertTrace(
        vector: Map<String, Any?>,
        bytes: ByteArray,
    ) {
        if (vector["reject"] == true) {
            assertFailsWith<SpanExportRefusal> { OtlpCodec.decodeTraceEvents(bytes) }
            return
        }
        val events = OtlpCodec.decodeTraceEvents(bytes)
        val expected = vector.getValue("events") as List<Map<String, Any?>>
        assertEquals(expected.size, events.size, vector["id"] as String)
        for ((event, want) in events.zip(expected)) {
            assertEquals(want["id"], event.id, vector["id"] as String)
            assertEquals(want["kind"], event.kind, vector["id"] as String)
            assertEquals((want.getValue("sequence_epoch") as String).toLong(), event.sequence.epoch)
            assertEquals((want.getValue("sequence_number") as String).toLong(), event.sequence.sequence)
            assertEquals(want["predecessors"], event.causalPredecessorIdsList, vector["id"] as String)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertCompleteness(
        vector: Map<String, Any?>,
        bytes: ByteArray,
    ) {
        val want = vector.getValue("completeness") as Map<String, String>
        val record = onlyLog(bytes)
        assertEquals("sentinel.model.v1.ProducerCompletenessSignal", logString(record, "sentinel.payload.type"))
        assertEquals(want.getValue("source_handle"), logString(record, "sentinel.source_handle"))
        val signal = ProducerCompletenessSignal.parseFrom(record.body.bytesValue)
        assertEquals(want.getValue("sequence_epoch").toLong(), signal.sequenceEpoch)
        assertEquals(want.getValue("last_emitted").toLong(), signal.lastEmitted)
        assertEquals(want.getValue("complete_through").toLong(), signal.completeThrough)
        assertEquals(want.getValue("acknowledged_filter_epoch").toLong(), signal.acknowledgedFilterEpoch)
        assertEquals(want.getValue("clock_domain_id"), signal.producerReportedAt.clockDomainId)
        assertEquals(BigInteger(want.getValue("occurrence_ns")), Int128Codec.toBigInt(signal.producerReportedAt.nanoseconds))
        assertEquals(want.getValue("uncertainty_nanoseconds").toLong(), signal.producerReportedAt.uncertaintyNanoseconds)
        assertEquals(activity(want.getValue("activity")), signal.activity)
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertVerdict(
        vector: Map<String, Any?>,
        bytes: ByteArray,
    ) {
        val want = vector.getValue("verdict") as Map<String, String>
        val decoded = OtlpCodec.decodeInbound(bytes).single() as InboundLog.VerdictLog
        assertEquals(want.getValue("id"), decoded.verdict.id)
        assertEquals(outcome(want.getValue("outcome")), decoded.verdict.outcome)
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertFilter(
        vector: Map<String, Any?>,
        bytes: ByteArray,
    ) {
        val want = vector.getValue("event_filter") as Map<String, Any?>
        val decoded = OtlpCodec.decodeInbound(bytes).single() as InboundLog.FilterLog
        assertEquals((want.getValue("epoch") as String).toLong(), decoded.filter.epoch)
        assertEquals(want.getValue("specification_ids"), decoded.filter.specificationsList.map { it.specificationId })
    }

    @Suppress("UNCHECKED_CAST")
    private fun assertDecision(
        vector: Map<String, Any?>,
        bytes: ByteArray,
    ) {
        val want = vector.getValue("decide_response") as Map<String, Any?>
        val decoded = OtlpCodec.decodeInbound(bytes).single() as InboundLog.DecisionLog
        assertEquals(want.getValue("request_id"), decoded.response.requestId)
        assertEquals(action(want.getValue("action") as String), decoded.response.action)
        val specs = want.getValue("specifications") as List<Map<String, String>>
        assertEquals(specs.size, decoded.response.specificationsCount)
        for ((spec, got) in specs.zip(decoded.response.specificationsList)) {
            assertEquals(spec.getValue("id"), got.specificationId)
            assertEquals(action(spec.getValue("action")), got.action)
        }
    }

    private fun onlyLog(bytes: ByteArray): LogRecord {
        val records =
            ExportLogsServiceRequest
                .parseFrom(bytes)
                .resourceLogsList
                .flatMap { resource -> resource.scopeLogsList.flatMap { it.logRecordsList } }
        assertEquals(1, records.size)
        return records[0]
    }

    private fun logString(
        record: LogRecord,
        key: String,
    ): String? =
        record.attributesList
            .firstOrNull { it.key == key }
            ?.value
            ?.stringValue

    private fun activity(token: String): SourceActivityState =
        when (token) {
            "active" -> SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE
            "idle" -> SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
            else -> error(token)
        }

    private fun outcome(token: String): VerdictOutcome =
        when (token) {
            "satisfy" -> VerdictOutcome.VERDICT_OUTCOME_SATISFY
            "violate" -> VerdictOutcome.VERDICT_OUTCOME_VIOLATE
            "inconclusive" -> VerdictOutcome.VERDICT_OUTCOME_INCONCLUSIVE
            else -> error(token)
        }

    private fun action(token: String): DecisionAction =
        when (token) {
            "permit" -> DecisionAction.DECISION_ACTION_PERMIT
            "deny" -> DecisionAction.DECISION_ACTION_DENY
            "defer" -> DecisionAction.DECISION_ACTION_DEFER
            else -> error(token)
        }
}
