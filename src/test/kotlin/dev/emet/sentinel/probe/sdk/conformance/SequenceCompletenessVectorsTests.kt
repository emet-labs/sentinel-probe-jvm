package dev.emet.sentinel.probe.sdk.conformance

import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.SourceActivityState
import dev.emet.sentinel.probe.sdk.config.JsonParser
import dev.emet.sentinel.probe.sdk.emission.FilteredRange
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.SequenceException
import dev.emet.sentinel.probe.sdk.emission.Slot
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import dev.emet.sentinel.probe.sdk.otlp.CompletenessHeartbeat
import dev.emet.sentinel.probe.sdk.otlp.CompletenessReport
import dev.emet.sentinel.probe.sdk.otlp.OtlpTransportException
import dev.emet.sentinel.probe.sdk.otlp.PartialSuccess
import java.math.BigInteger
import java.nio.file.Path
import java.util.HexFormat
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Executes the shared sequence_completeness suite (#461) through the real SequenceAllocator,
// CompletenessHeartbeat and PartialSuccess decoder. The suite is found through the manifest.
class SequenceCompletenessVectorsTests {
    private val root =
        Path
            .of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/probe-sdk-conformance")

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
        assertEquals("1.3.0", manifest["format_version"])
        val entry = (manifest["suites"] as List<Map<String, String>>).single { it["kind"] == "sequence_completeness" }
        val suite = load(entry.getValue("path"))
        assertEquals("1.3.0", suite["format_version"])
        assertEquals("sequence_completeness", suite["kind"])
        val cases = suite["cases"] as List<Map<String, Any?>>
        assertEquals(cases.size, cases.map { it["id"] }.toSet().size, "case ids are unique")
        for (case in cases) {
            assertTrue(case.containsKey("steps") != case.containsKey("response_hex"), "${case["id"]} has one shape")
        }
        return cases
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `sequence_completeness scenarios drive the allocator and the heartbeat`() {
        val scenarios = cases().filter { it.containsKey("steps") }
        assertTrue(scenarios.isNotEmpty())
        for (case in scenarios) {
            val start = case.getValue("start") as Map<String, Any?>
            val scenario = Scenario(case.getValue("id") as String, start)
            for ((index, step) in (case.getValue("steps") as List<Map<String, Any?>>).withIndex()) {
                scenario.step(index, step)
            }
        }
    }

    @Test
    fun `sequence_completeness partial-success cases decode rejected_spans`() {
        val responses = cases().filter { it.containsKey("response_hex") }
        assertTrue(responses.isNotEmpty())
        for (case in responses) {
            val bytes = HexFormat.of().parseHex(case.getValue("response_hex") as String)
            val id = case.getValue("id") as String
            if (case["undecodable"] == true) {
                assertFailsWith<IllegalArgumentException>(id) { PartialSuccess.rejectedSpans(bytes) }
            } else {
                val rejected = (case.getValue("rejected_spans") as String).toLong()
                assertEquals(rejected, PartialSuccess.rejectedSpans(bytes), id)
            }
        }
    }

    // Scenario is one allocator and one heartbeat over an injected clock, Filter-epoch reader
    // and send function. A restart replaces both unless it is refused.
    private class Scenario(
        private val id: String,
        start: Map<String, Any?>,
    ) {
        private val clockNanos = AtomicLong()
        private var filterEpoch: Long? = null
        private val outcomes = ArrayDeque<String>()
        private val sent = ArrayList<ProducerCompletenessSignal>()
        private val errors = ArrayList<Throwable>()
        private var previousSignal: ProducerCompletenessSignal? = null
        private var allocator: SequenceAllocator
        private var heartbeat: CompletenessHeartbeat

        init {
            setClock(start.getValue("clock_unix_ms") as String)
            allocator = SequenceAllocator(clockNanos::get, uint(start["epoch"]), uint(start["previous_epoch"]))
            heartbeat = heartbeatFor(allocator)
        }

        @Suppress("UNCHECKED_CAST")
        fun step(
            index: Int,
            step: Map<String, Any?>,
        ) {
            val where = "$id step $index ${step["op"]}"
            when (step.getValue("op")) {
                "allocate" -> allocate(where, step)
                "export_ok" -> allocator.markExported(slot(step))
                "export_fail" -> allocator.markFailed(slot(step))
                "filter_drop" -> allocator.markFiltered(slot(step), uint(step["filter_epoch"])!!)
                "set_filter_epoch" -> filterEpoch = uint(step["epoch"])
                "clock" -> setClock(step.getValue("unix_ms") as String)
                "idle" -> heartbeat.declareIdle()
                "active" -> heartbeat.declareActive()
                "tick" -> report(where, step) { heartbeat.report() }
                "shutdown" -> report(where, step) { heartbeat.shutdown() }
                "restart" -> restart(where, step)
                else -> error("$where: unknown op")
            }
        }

        private fun allocate(
            where: String,
            step: Map<String, Any?>,
        ) {
            val code = step["expect_error"] as String?
            if (code != null) {
                assertEquals(code, assertFailsWith<SequenceException>(where) { allocator.allocate() }.code, where)
                return
            }
            @Suppress("UNCHECKED_CAST")
            val expect = step.getValue("expect") as Map<String, String>
            assertEquals(Slot(expect.getValue("epoch").toLong(), expect.getValue("number").toLong()), allocator.allocate(), where)
        }

        @Suppress("UNCHECKED_CAST")
        private fun report(
            where: String,
            step: Map<String, Any?>,
            call: () -> List<CompletenessReport>,
        ) {
            val sends = step.getValue("send") as List<String>
            outcomes.clear()
            outcomes.addAll(sends)
            sent.clear()
            errors.clear()
            val reports = call()
            val expected = step.getValue("expect") as List<Map<String, Any?>>
            assertEquals(expected.size, sent.size, "$where: signals sent")
            assertEquals(sends.map { it == "accepted" }, reports.map { it.accepted }, "$where: accepted")
            for ((position, pair) in sent.zip(expected).withIndex()) {
                assertSignal("$where signal $position", pair.first, pair.second)
            }
            val codes = errors.filterIsInstance<SequenceException>().map { it.code }
            assertEquals(listOfNotNull(step["expect_error"] as String?), codes, "$where: error codes")
            assertEquals(sends.count { it == "not_accepted" }, errors.count { it is OtlpTransportException }, "$where: refusals")
            val filtered =
                (step.getValue("filtered") as List<Map<String, String>>).map {
                    FilteredRange(it.getValue("first").toLong(), it.getValue("last").toLong(), it.getValue("filter_epoch").toLong())
                }
            assertEquals(filtered, allocator.declaration().filtered, "$where: filtered")
        }

        private fun restart(
            where: String,
            step: Map<String, Any?>,
        ) {
            setClock(step.getValue("clock_unix_ms") as String)
            val previous =
                when (val value = step["previous_epoch"]) {
                    "inherit" -> allocator.epoch()
                    else -> uint(value)
                }
            val code = step["expect_error"] as String?
            if (code != null) {
                val refusal =
                    assertFailsWith<SequenceException>(where) {
                        SequenceAllocator(clockNanos::get, uint(step["epoch"]), previous)
                    }
                assertEquals(code, refusal.code, where)
                return
            }
            heartbeat.stop()
            allocator = SequenceAllocator(clockNanos::get, uint(step["epoch"]), previous)
            heartbeat = heartbeatFor(allocator)
        }

        private fun assertSignal(
            where: String,
            signal: ProducerCompletenessSignal,
            want: Map<String, Any?>,
        ) {
            assertEquals((want.getValue("sequence_epoch") as String).toLong(), signal.sequenceEpoch, "$where sequence_epoch")
            assertTrue(signal.hasLastEmitted() && signal.hasCompleteThrough() && signal.hasAcknowledgedFilterEpoch(), "$where presence")
            assertEquals((want.getValue("last_emitted") as String).toLong(), signal.lastEmitted, "$where last_emitted")
            assertEquals((want.getValue("complete_through") as String).toLong(), signal.completeThrough, "$where complete_through")
            assertEquals(
                (want.getValue("acknowledged_filter_epoch") as String).toLong(),
                signal.acknowledgedFilterEpoch,
                "$where acknowledged_filter_epoch",
            )
            assertEquals(activity(want.getValue("activity") as String), signal.activity, "$where activity")
            for ((time, key) in listOf(signal.producerReportedAt to "reported_at_ns", signal.activitySince to "activity_since_ns")) {
                assertEquals("unix", time.clockDomainId, "$where $key clock domain")
                assertEquals(0L, time.uncertaintyNanoseconds, "$where $key uncertainty")
                assertEquals(BigInteger(want.getValue(key) as String), Int128Codec.toBigInt(time.nanoseconds), "$where $key")
            }
            assertEquals(want.getValue("restates_previous"), restates(signal), "$where restates_previous")
            previousSignal = signal
        }

        // restates re-encodes the signal with the previous signal's report nanoseconds and
        // compares the bytes, which is what Sentinel's restatement check ignores.
        private fun restates(signal: ProducerCompletenessSignal): Boolean {
            val previous = previousSignal ?: return false
            val aligned =
                signal
                    .toBuilder()
                    .setProducerReportedAt(
                        signal.producerReportedAt.toBuilder().setNanoseconds(previous.producerReportedAt.nanoseconds),
                    ).build()
            return aligned.toByteArray().contentEquals(previous.toByteArray())
        }

        private fun heartbeatFor(sequence: SequenceAllocator): CompletenessHeartbeat =
            CompletenessHeartbeat(
                sequence = sequence,
                send = { signal ->
                    sent.add(signal)
                    val outcome = outcomes.removeFirstOrNull() ?: "unscripted"
                    if (outcome != "accepted") throw OtlpTransportException("$id: send $outcome")
                },
                filterEpoch = { filterEpoch },
                nowUnixNanos = clockNanos::get,
                intervalMillis = 0L,
                onError = { errors.add(it) },
            )

        private fun setClock(millis: String) {
            clockNanos.set(Math.multiplyExact(millis.toLong(), NANOS_PER_MILLI))
        }

        private fun slot(step: Map<String, Any?>): Slot = Slot(uint(step["epoch"])!!, uint(step["number"])!!)

        private fun uint(value: Any?): Long? = (value as String?)?.toLong()

        private fun activity(token: String): SourceActivityState =
            when (token) {
                "active" -> SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE
                "idle" -> SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
                else -> error(token)
            }

        private companion object {
            const val NANOS_PER_MILLI = 1_000_000L
        }
    }
}
