package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.Int128
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.SourceActivityState
import dev.emet.sentinel.model.v1.VerdictOutcome
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.sdk.common.InstrumentationScopeInfo
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.testing.trace.TestSpanData
import io.opentelemetry.sdk.trace.data.SpanData
import io.opentelemetry.sdk.trace.data.StatusData
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import java.io.File
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SameMachineVerdictTests {
    private val http: HttpClient = HttpClient.newHttpClient()
    private val startNanos = 1_700_000_000_000_000_000L

    @Test
    fun `linked charge and ship observe a verdict`() {
        val bin = peripheryBin()
        OtlpLogReceiver().use { receiver ->
            val periphery = startPeriphery(bin, receiver.logUrl)
            try {
                coverAndBootstrap(periphery.baseUrl)
                OtlpHttpExporter(periphery.baseUrl, "sagashop-payment").exportSpan(
                    sampleSpan(
                        name = "sagashop.payment.charge.completed",
                        eventId = "01f0c0ffee-charge",
                        sequence = 1,
                        parentId = null,
                        attributes =
                            listOf(
                                "sagashop.payment.status" to "success",
                                "sagashop.order.id" to "order-1001",
                            ),
                    ),
                )
                OtlpHttpExporter(periphery.baseUrl, "sagashop-order").exportSpan(
                    sampleSpan(
                        name = "sagashop.order.ship.requested",
                        eventId = "01f0c0ffee-ship",
                        sequence = 1,
                        parentId = "01f0c0ffee-charge",
                        attributes = listOf("sagashop.order.id" to "order-1001"),
                    ),
                )
                evaluationsSettled(periphery.baseUrl)
                val verdicts = receiver.verdicts()
                assertTrue(verdicts.isNotEmpty(), "the log receiver must observe a Verdict")
                assertTrue(verdicts.any { it.id.contains("sagashop.p4.ship-requires-prior-charge") })
                assertTrue(verdicts.any { it.outcome == VerdictOutcome.VERDICT_OUTCOME_SATISFY })
            } finally {
                periphery.process.destroyForcibly()
            }
        }
    }

    @Test
    fun `a ship without a charge is not Satisfy`() {
        val bin = peripheryBin()
        OtlpLogReceiver().use { receiver ->
            val periphery = startPeriphery(bin, receiver.logUrl)
            try {
                coverAndBootstrap(periphery.baseUrl)
                OtlpHttpExporter(periphery.baseUrl, "sagashop-payment").exportSpan(
                    sampleSpan(
                        name = "sagashop.payment.charge.completed",
                        eventId = "01f0c0ffee-declin",
                        sequence = 1,
                        parentId = null,
                        attributes =
                            listOf(
                                "sagashop.payment.status" to "declined",
                                "sagashop.order.id" to "order-2002",
                            ),
                    ),
                )
                OtlpHttpExporter(periphery.baseUrl, "sagashop-order").exportSpan(
                    sampleSpan(
                        name = "sagashop.order.ship.requested",
                        eventId = "01f0c0ffee-noship",
                        sequence = 1,
                        parentId = null,
                        attributes = listOf("sagashop.order.id" to "order-2002"),
                    ),
                )
                evaluationsSettled(periphery.baseUrl)
                val verdicts = receiver.verdicts()
                assertTrue(verdicts.isNotEmpty(), "a covered ship with no successful charge still returns a Verdict")
                assertTrue(verdicts.none { it.outcome == VerdictOutcome.VERDICT_OUTCOME_SATISFY })
            } finally {
                periphery.process.destroyForcibly()
            }
        }
    }

    // Periphery evaluates after it acknowledges an OTLP post, and pushes the Verdict from its
    // evaluation worker (#454). Read `requested` once from GET /v1/evaluations/queue and poll
    // until `completed` reaches it, so the receiver has seen every push the posts caused.
    private fun evaluationsSettled(baseUrl: String) {
        var target: Long? = null
        repeat(400) {
            val request = HttpRequest.newBuilder(URI.create("$baseUrl/v1/evaluations/queue")).GET().build()
            val response = http.send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, response.statusCode(), response.body())
            val goal = target ?: counter(response.body(), "requested").also { target = it }
            if (counter(response.body(), "completed") >= goal) {
                // A failed pass still completes its requests; this stack never fails one.
                assertEquals(0L, counter(response.body(), "failed_passes"), response.body())
                return
            }
            Thread.sleep(10)
        }
        throw AssertionError("evaluations did not settle")
    }

    private fun counter(
        body: String,
        name: String,
    ): Long =
        Regex("\"$name\":(\\d+)")
            .find(body)
            ?.groupValues
            ?.get(1)
            ?.toLong()
            ?: throw AssertionError("no $name in $body")

    private fun peripheryBin(): String {
        val bin = System.getProperty("sentinel.periphery.bin")
        Assumptions.assumeTrue(!bin.isNullOrBlank()) { "SENTINEL_PERIPHERY_BIN is unset" }
        assertTrue(File(bin).canExecute(), bin)
        return bin
    }

    private class Running(
        val process: Process,
        val baseUrl: String,
    )

    private fun startPeriphery(
        bin: String,
        probeLogUrl: String,
    ): Running {
        val port = ServerSocket(0).use { it.localPort }
        val baseUrl = "http://127.0.0.1:$port"
        val process =
            ProcessBuilder(bin)
                .redirectErrorStream(true)
                .apply {
                    environment()["SENTINEL_STORE"] = "memory"
                    environment()["SENTINEL_LISTEN"] = "127.0.0.1:$port"
                    environment()["SENTINEL_PROBE_LOG_URL"] = probeLogUrl
                }.start()
        val output = StringBuilder()
        val reader =
            Thread {
                process.inputStream.bufferedReader().useLines { lines ->
                    for (line in lines) {
                        synchronized(output) { output.append(line).append('\n') }
                    }
                }
            }
        reader.isDaemon = true
        reader.start()
        val deadline = System.nanoTime() + 20_000_000_000L
        while (System.nanoTime() < deadline) {
            if (!process.isAlive) {
                error("sentinel-periphery exited: $output")
            }
            val text = synchronized(output) { output.toString() }
            if (text.contains("listening on")) return Running(process, baseUrl)
            Thread.sleep(20)
        }
        process.destroyForcibly()
        error("sentinel-periphery did not listen: $output")
    }

    private fun coverAndBootstrap(base: String) {
        for (service in listOf("sagashop-order", "sagashop-payment")) {
            val exporter = OtlpHttpExporter(base, service)
            exporter.exportCompleteness(signal(1))
        }
        val body =
            """{"observed_at":{"sentinel_clock_id":"sentinel","nanoseconds":1700000000000000000}}"""
        val response =
            http.send(
                HttpRequest
                    .newBuilder()
                    .uri(URI.create("$base/v1/filters/bootstrap-cat1"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertEquals(200, response.statusCode(), response.body())
    }

    private fun signal(last: Long): ProducerCompletenessSignal {
        val whenNanos =
            OccurrenceTime
                .newBuilder()
                .setClockDomainId("unix")
                .setNanoseconds(
                    Int128
                        .newBuilder()
                        .setHigh(0)
                        .setLow(startNanos)
                        .build(),
                ).setUncertaintyNanoseconds(0)
                .build()
        return ProducerCompletenessSignal
            .newBuilder()
            .setSequenceEpoch(7L)
            .setLastEmitted(last)
            .setCompleteThrough(last)
            .setProducerReportedAt(whenNanos)
            .setAcknowledgedFilterEpoch(1L)
            .setActivity(SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE)
            .setActivitySince(whenNanos)
            .build()
    }

    private fun sampleSpan(
        name: String,
        eventId: String,
        sequence: Long,
        parentId: String?,
        attributes: List<Pair<String, String>>,
    ): SpanData {
        val builder = Attributes.builder()
        builder.put(AttributeKey.stringKey(ATTRIBUTE_EVENT_ID), eventId)
        builder.put(AttributeKey.stringKey(ATTRIBUTE_SCHEMA_VERSION), "sagashop-contract-v1")
        builder.put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH), 7L)
        builder.put(AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER), sequence)
        builder.put(AttributeKey.longKey(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH), 1L)
        builder.put(AttributeKey.stringKey(ATTRIBUTE_CLAIMED_SENSITIVITY), "SENSITIVITY_INTERNAL")
        if (parentId != null) builder.put(AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID), parentId)
        for ((key, value) in attributes) builder.put(AttributeKey.stringKey(key), value)
        val attributesBuilt = builder.build()
        val context =
            SpanContext.create(
                "0102030405060708090a0b0c0d0e0f10",
                "3132333435363738",
                TraceFlags.getDefault(),
                TraceState.getDefault(),
            )
        return TestSpanData
            .builder()
            .setName(name)
            .setKind(SpanKind.INTERNAL)
            .setSpanContext(context)
            .setParentSpanContext(SpanContext.getInvalid())
            .setStartEpochNanos(startNanos)
            .setEndEpochNanos(startNanos + 5_000_000)
            .setAttributes(attributesBuilt)
            .setStatus(StatusData.unset())
            .setHasEnded(true)
            .setTotalRecordedEvents(0)
            .setTotalRecordedLinks(0)
            .setTotalAttributeCount(attributesBuilt.size())
            .setResource(Resource.getDefault())
            .setInstrumentationScopeInfo(InstrumentationScopeInfo.empty())
            .build()
    }
}
