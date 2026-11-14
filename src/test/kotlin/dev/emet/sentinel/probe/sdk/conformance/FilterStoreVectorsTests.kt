package dev.emet.sentinel.probe.sdk.conformance

import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.FilterStore
import dev.emet.sentinel.probe.sdk.config.JsonParser
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

// Executes the shared filter_store suite (manifest-v1.json, 1.4.0) through the production
// FilterStore. Epochs are decimal strings parsed as unsigned 64-bit values into a Long, the
// carrier FilterStore compares with java.lang.Long.compareUnsigned.
class FilterStoreVectorsTests {
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

    private fun epoch(value: Any?): Long? = (value as String?)?.let(java.lang.Long::parseUnsignedLong)

    private fun build(fixture: Map<*, *>): EventFilter {
        val builder =
            EventFilter
                .newBuilder()
                .addSpecifications(SpecificationFilter.newBuilder().setSpecificationId(fixture["label"] as String))
        epoch(fixture["epoch"])?.let(builder::setEpoch)
        return builder.build()
    }

    // Applies one set step. Returns the Filter last applied and whether the held reference is
    // still asserted: an equal-epoch set pins only content, so tracking stops after it.
    @Suppress("UNCHECKED_CAST")
    private fun runSet(
        store: FilterStore,
        step: Map<String, Any?>,
        applied: EventFilter?,
        trackReference: Boolean,
        at: String,
    ): Pair<EventFilter?, Boolean> {
        val offered = build(step["filter"] as Map<*, *>)
        val replaced = store.set(offered)
        val expected = step["expected"] as Map<String, Any?>
        val pinned = expected.containsKey("applied")
        if (pinned) assertEquals(expected["applied"] as Boolean, replaced, at)
        val nowApplied = if (pinned && replaced) offered else applied
        val nowTracked = trackReference && pinned
        val held = requireNotNull(store.get()) { at }
        if (nowTracked) assertSame(nowApplied, held, at)
        val want = expected["held"] as Map<*, *>
        assertEquals(listOf(want["label"]), held.specificationsList.map { it.specificationId }, at)
        val wantEpoch = epoch(want["epoch"])
        assertEquals(wantEpoch != null, held.hasEpoch(), at)
        assertEquals(wantEpoch, store.epoch(), at)
        return nowApplied to nowTracked
    }

    @Suppress("UNCHECKED_CAST")
    private fun runCase(vector: Map<String, Any?>) {
        var applied = (vector["initial"] as Map<*, *>?)?.let(::build)
        val store = FilterStore.newFilterStore(applied)
        var trackReference = true
        for ((index, step) in (vector["steps"] as List<Map<String, Any?>>).withIndex()) {
            val at = "${vector["id"]} step $index"
            when (step["op"]) {
                "should_refresh" ->
                    assertEquals(step["expected"] as Boolean, store.shouldRefresh(epoch(step["epoch"])), at)
                "set" -> {
                    val (nowApplied, nowTracked) = runSet(store, step, applied, trackReference, at)
                    applied = nowApplied
                    trackReference = nowTracked
                }
                else -> error("$at: unknown op ${step["op"]}")
            }
        }
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `filter_store vectors replay the production store`() {
        val manifest = load("manifest-v1.json")
        assertEquals("1.4.0", manifest["format_version"])
        val entry = (manifest["suites"] as List<Map<String, String>>).single { it["kind"] == "filter_store" }
        val suite = load(entry.getValue("path"))
        assertEquals("1.2.0", suite["format_version"])
        assertEquals("filter_store", suite["kind"])
        val cases = suite["cases"] as List<Map<String, Any?>>
        assertTrue(cases.isNotEmpty())
        cases.forEach(::runCase)
    }
}
