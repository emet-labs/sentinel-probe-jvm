package dev.emet.sentinel.probe.sdk.conformance

import dev.emet.sentinel.model.v1.Int128
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.config.JsonParser
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import dev.emet.sentinel.probe.sdk.internal.specmatch.SpecMatch
import java.math.BigInteger
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ConformanceVectorsTests {
    private val root =
        Path.of(requireNotNull(System.getProperty("sentinel.repository.root")))
            .resolve("testdata/probe-sdk-conformance")

    @Suppress("UNCHECKED_CAST")
    private fun load(name: String): Map<String, Any?> {
        val parser = JsonParser(root.resolve(name).toFile().readText())
        val value = parser.parseValue() as Map<String, Any?>
        parser.skipWhitespace()
        parser.expectEof()
        assertEquals("1.0.0", value["format_version"])
        return value
    }

    @Test
    fun `manifest suite registry fails closed`() {
        val manifest = load("manifest-v1.json")
        val suites = manifest["suites"] as List<Map<String, String>>
        assertEquals(
            listOf("spec_match", "int128", "enforcement_gate"),
            suites.map { it.getValue("kind") },
        )
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
            val words = Int128.newBuilder().setHigh(high).setLow(low.toLong()).build()
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
                val matchBuilder = dev.emet.sentinel.model.v1.EventMatch.newBuilder()
                matchBuilder.addAllEventKinds(match["event_kinds"] as List<String>)
                matchBuilder.addAllProjectedAttributeKeys(match["projected_attribute_keys"] as List<String>)
                builder.eventMatch = matchBuilder.build()
            }
            val eventMap = vector["producer_event"] as Map<String, Any?>
            val event = ProducerEvent.newBuilder().setKind(eventMap["kind"] as String).build()
            assertEquals(vector["expected"], SpecMatch.selects(builder.build(), event), vector["id"] as String)
        }
    }
}
