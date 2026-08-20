package dev.emet.sentinel.probe.sdk.filter

import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.internal.specmatch.SpecMatch
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.bind
import io.kotest.property.arbitrary.constant
import io.kotest.property.arbitrary.list
import io.kotest.property.arbitrary.of
import io.kotest.property.arbitrary.string
import io.kotest.property.checkAll
import org.junit.jupiter.api.Test

class ApplyFilterPropertyTests {
    // A pinned seed keeps the property run hermetic (ADR-0019): no wall clock drives the RNG,
    // so a failure reproduces deterministically. Mirrors the Go rapid -rapid.seed pin.
    private val config = PropTestConfig(seed = 31L)

    private val attrKey: Arb<String> = Arb.string(1, 4)
    private val kind: Arb<String> = Arb.of(listOf("transfer.initiated", "approval.granted", "order.placed", "x"))

    private fun stringAttr(
        key: String,
        value: String,
    ): AttributeEntry =
        AttributeEntry
            .newBuilder()
            .setKey(key)
            .setValue(AttributeValue.newBuilder().setStringValue(value).build())
            .build()

    private val event: Arb<ProducerEvent> =
        Arb.bind(
            Arb.list(attrKey, 0..4),
            Arb.list(kind, 1..2),
        ) { keys, predecessors ->
            val distinctKeys = keys.distinct()
            ProducerEvent
                .newBuilder()
                .setId("evt")
                .setKind(if (predecessors.isNotEmpty()) predecessors.first() else "x")
                .setSchemaVersion("sentinel.model.v1")
                .addAllAttributes(distinctKeys.mapIndexed { i, k -> stringAttr(k, "v$i") })
                .addAllCausalPredecessorIds(predecessors)
                .build()
        }

    private val filter: Arb<EventFilter> =
        Arb.bind(
            Arb.list(kind, 0..3),
            Arb.list(attrKey, 0..3),
        ) { kinds, projected ->
            val spec =
                SpecificationFilter
                    .newBuilder()
                    .setEventMatch(
                        EventMatch
                            .newBuilder()
                            .addAllEventKinds(kinds.distinct())
                            .addAllProjectedAttributeKeys(projected.distinct())
                            .setDeliveryMode(DeliveryMode.DELIVERY_MODE_SHIP_ASYNC)
                            .build(),
                    ).build()
            EventFilter
                .newBuilder()
                .setEpoch(5L)
                .addSpecifications(spec)
                .build()
        }

    private fun selects(
        eventKind: String,
        specKinds: List<String>,
    ): Boolean = specKinds.isEmpty() || specKinds.contains(eventKind)

    @Test
    fun `projection is sound - preserves kind, causal edges and selected attributes`() {
        // checkAll is a suspend function, so runBlocking drives the property from a plain JUnit
        // test. The pinned seed (config) keeps the run hermetic (ADR-0019).
        kotlinx.coroutines.runBlocking {
            checkAll(config, event, filter) { e, f ->
                val got = ApplyFilter.apply(e, f)
                val spec = f.specificationsList.first()
                val kinds = spec.eventMatch.eventKindsList
                val projected = spec.eventMatch.projectedAttributeKeysList
                if (!selects(e.kind, kinds)) {
                    org.junit.jupiter.api.Assertions
                        .assertNull(got, "non-selecting spec must drop")
                    return@checkAll
                }
                org.junit.jupiter.api.Assertions
                    .assertNotNull(got, "selecting spec must not drop")
                val out = got!!
                org.junit.jupiter.api.Assertions
                    .assertEquals(e.kind, out.kind)
                org.junit.jupiter.api.Assertions.assertEquals(
                    e.causalPredecessorIdsList,
                    out.causalPredecessorIdsList,
                    "causal skeleton must survive",
                )
                val byKey = out.attributesList.associateBy { it.key }
                if (projected.isEmpty()) {
                    // keep-everything: every input attribute must survive.
                    for (entry in e.attributesList) {
                        org.junit.jupiter.api.Assertions
                            .assertTrue(entry.key in byKey, "keep-everything lost ${entry.key}")
                    }
                } else {
                    for (key in projected) {
                        if (e.attributesList.any { it.key == key }) {
                            org.junit.jupiter.api.Assertions
                                .assertTrue(key in byKey, "projected key $key was trimmed")
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `causal predecessors survive whenever the event survives`() {
        kotlinx.coroutines.runBlocking {
            checkAll(config, event, filter) { e, f ->
                val got = ApplyFilter.apply(e, f)
                if (got != null) {
                    org.junit.jupiter.api.Assertions
                        .assertEquals(e.causalPredecessorIdsList, got.causalPredecessorIdsList)
                }
            }
        }
    }
}
