package dev.emet.sentinel.probe.sdk.filter

import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.model.v1.SpecificationFilter
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ApplyFilterTests {
    private fun attr(key: String, value: String): AttributeEntry =
        AttributeEntry.newBuilder()
            .setKey(key)
            .setValue(AttributeValue.newBuilder().setStringValue(value).build())
            .build()

    private fun event(kind: String, attrs: List<AttributeEntry>, vararg predecessors: String): ProducerEvent =
        ProducerEvent.newBuilder()
            .setId("test-event")
            .setKind(kind)
            .setSchemaVersion("sentinel.model.v1")
            .addAllAttributes(attrs)
            .addAllCausalPredecessorIds(predecessors.toList())
            .build()

    private fun spec(kinds: List<String>, keys: List<String>): SpecificationFilter =
        SpecificationFilter.newBuilder()
            .setSpecificationId("spec-1")
            .setSpecificationVersion("1.0.0")
            .setEventMatch(
                EventMatch.newBuilder()
                    .addAllEventKinds(kinds)
                    .addAllProjectedAttributeKeys(keys)
                    .setDeliveryMode(DeliveryMode.DELIVERY_MODE_SHIP_ASYNC)
                    .build(),
            )
            .build()

    private fun eventFilter(vararg specs: SpecificationFilter): EventFilter =
        EventFilter.newBuilder().setEpoch(5L).addAllSpecifications(specs.toList()).build()

    private fun keysOf(event: ProducerEvent): List<String> = event.attributesList.map { it.key }

    @Test
    fun `no spec selects drops the event`() {
        val got = ApplyFilter.apply(
            event("transfer.initiated", listOf(attr("amount", "100"))),
            eventFilter(spec(listOf("approval.granted"), listOf("approver"))),
        )
        assertNull(got)
    }

    @Test
    fun `projects a subset of attributes`() {
        val got = ApplyFilter.apply(
            event(
                "approval.granted",
                listOf(attr("approver", "alice"), attr("binding_id", "b-1"), attr("amount", "1000")),
            ),
            eventFilter(spec(listOf("approval.granted"), listOf("approver", "binding_id"))),
        )
        assertTrue(got != null, "a selecting spec must not drop the event")
        val keys = keysOf(got!!)
        assertEquals(listOf("approver", "binding_id").sorted(), keys.sorted())
        assertFalse("amount" in keys, "an unprojected attribute must be trimmed")
    }

    @Test
    fun `empty projection keeps all attributes`() {
        val got = ApplyFilter.apply(
            event("approval.granted", listOf(attr("approver", "alice"), attr("amount", "1000"))),
            eventFilter(spec(listOf("approval.granted"), emptyList())),
        )
        assertTrue(got != null && got.attributesList.size == 2, "an empty projection set must keep every attribute")
    }

    @Test
    fun `unions projected keys across specs`() {
        val got = ApplyFilter.apply(
            event("transfer.initiated", listOf(attr("amount", "100"), attr("account", "acc-1"), attr("other", "x"))),
            eventFilter(
                spec(listOf("transfer.initiated"), listOf("amount")),
                spec(emptyList(), listOf("account")), // empty event_kinds matches every kind
            ),
        )
        assertTrue(got != null)
        val keys = keysOf(got!!)
        assertTrue("amount" in keys && "account" in keys, "keys = $keys, want the union of both projections")
        assertFalse("other" in keys, "a key outside the union must be trimmed")
    }

    @Test
    fun `project-all wins over subset`() {
        val got = ApplyFilter.apply(
            event("x", listOf(attr("a", "1"), attr("b", "2"))),
            eventFilter(spec(listOf("x"), emptyList()), spec(listOf("x"), listOf("a"))),
        )
        assertTrue(got != null && got.attributesList.size == 2, "one project-all spec must over-approximate upward")
    }

    @Test
    fun `empty filter drops`() {
        val got = ApplyFilter.apply(event("anything", listOf(attr("k", "v"))), eventFilter())
        assertNull(got)
    }

    @Test
    fun `null filter drops`() {
        assertNull(ApplyFilter.apply(event("anything", emptyList()), null))
    }

    @Test
    fun `empty event_kinds matches all and still projects`() {
        val got = ApplyFilter.apply(
            event("anything", listOf(attr("k", "v"), attr("drop", "x"))),
            eventFilter(spec(emptyList(), listOf("k"))),
        )
        assertTrue(got != null && got.attributesList.size == 1 && got.attributesList[0].key == "k")
    }

    @Test
    fun `never trims causal predecessors`() {
        val got = ApplyFilter.apply(
            event("x", listOf(attr("keep", "v")), "pred-a", "pred-b"),
            eventFilter(spec(listOf("x"), listOf("keep"))),
        )
        assertTrue(got != null)
        assertEquals(listOf("pred-a", "pred-b"), got!!.causalPredecessorIdsList)
    }

    @Test
    fun `drops an unprojected attribute`() {
        val got = ApplyFilter.apply(
            event("x", listOf(attr("keep", "v"), attr("drop", "x"))),
            eventFilter(spec(listOf("x"), listOf("keep"))),
        )
        assertTrue(got != null && got.attributesList.size == 1 && got.attributesList[0].key == "keep")
    }

    @Test
    fun `preserves identity fields`() {
        val source = event("approval.granted", listOf(attr("approver", "alice")), "parent-1")
        val got = ApplyFilter.apply(source, eventFilter(spec(listOf("approval.granted"), listOf("approver"))))
        assertTrue(got != null)
        assertEquals("approval.granted", got!!.kind)
        assertEquals("test-event", got.id)
        assertEquals("sentinel.model.v1", got.schemaVersion)
        assertEquals(listOf("parent-1"), got.causalPredecessorIdsList)
    }

    @Test
    fun `preserves acknowledged epoch including zero`() {
        data class Case(val epoch: Long?, val want: Boolean, val name: String)
        val cases = listOf(
            Case(0L, true, "epoch zero is present and kept"),
            Case(7L, true, "a positive epoch is kept"),
            Case(null, false, "an absent epoch stays absent"),
        )
        for (c in cases) {
            val source = ProducerEvent.newBuilder()
                .setId("evt")
                .setKind("x")
                .setSchemaVersion("sentinel.model.v1")
                .addAttributes(attr("k", "v"))
                .also { if (c.epoch != null) it.setAcknowledgedFilterEpoch(c.epoch) }
                .build()
            val got = ApplyFilter.apply(source, eventFilter(spec(listOf("x"), listOf("k"))))
            assertTrue(got != null, c.name)
            assertEquals(c.want, got!!.hasAcknowledgedFilterEpoch(), c.name)
            if (c.want) assertEquals(c.epoch!!, got.acknowledgedFilterEpoch, c.name)
        }
    }

    @Test
    fun `preserves capabilities, sensitivity and timing`() {
        val source = ProducerEvent.newBuilder()
            .setId("evt")
            .setKind("x")
            .setSchemaVersion("sentinel.model.v1")
            .setSequence(SequenceCoordinate.newBuilder().setEpoch(3L).setSequence(9L).build())
            .setOccurrenceTime(
                OccurrenceTime.newBuilder()
                    .setClockDomainId("unix")
                    .setNanoseconds(dev.emet.sentinel.model.v1.Int128.newBuilder().setHigh(0L).setLow(1700000000123456789L).build())
                    .build(),
            )
            .addClaimedCapabilities(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES)
            .setClaimedSensitivity(Sensitivity.SENSITIVITY_CONFIDENTIAL)
            .addAttributes(attr("k", "v"))
            .build()
        val got = ApplyFilter.apply(source, eventFilter(spec(listOf("x"), listOf("k"))))!!
        assertEquals(9L, got.sequence.sequence)
        assertEquals(3L, got.sequence.epoch)
        assertEquals("unix", got.occurrenceTime.clockDomainId)
        assertEquals(1700000000123456789L, got.occurrenceTime.nanoseconds.low)
        assertEquals(listOf(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES), got.claimedCapabilitiesList)
        assertEquals(Sensitivity.SENSITIVITY_CONFIDENTIAL, got.claimedSensitivity)
    }

    @Test
    fun `allocates a fresh attribute slice`() {
        // Pins the aliasing contract: the projected event must not share its attribute list with
        // the input. The entries themselves remain shared (shallow aliasing), matching the Go
        // reference's documented contract.
        val source = event("x", listOf(attr("a", "1"), attr("b", "2")))
        val got = ApplyFilter.apply(source, eventFilter(spec(listOf("x"), emptyList())))!!
        assertEquals(source.attributesList.size, got.attributesList.size)
        // Mutating the projected list must not affect the source.
        val swapped = attr("swapped", "z")
        // Build a new projected event with a swapped entry and confirm the source is untouched.
        val rebuilt = got.toBuilder().setAttributes(0, swapped).build()
        assertEquals("a", source.attributesList[0].key, "the projected event must not alias the input's attribute list")
        assertEquals("swapped", rebuilt.attributesList[0].key)
        // Entries are shared by design: the second entry reference is the same object.
        assertTrue(got.attributesList[1] === source.attributesList[1], "entries are shared by design")
    }
}
