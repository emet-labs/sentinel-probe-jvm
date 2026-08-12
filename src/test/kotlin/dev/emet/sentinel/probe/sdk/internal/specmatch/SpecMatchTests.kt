package dev.emet.sentinel.probe.sdk.internal.specmatch

import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class SpecMatchTests {
    private fun withKinds(vararg kinds: String): SpecificationFilter =
        SpecificationFilter.newBuilder()
            .setEventMatch(EventMatch.newBuilder().addAllEventKinds(kinds.toList()).build())
            .build()

    @Test
    fun `table of selection cases`() {
        data class Case(val name: String, val spec: SpecificationFilter?, val event: ProducerEvent?, val want: Boolean)

        val cases = listOf(
            Case("empty event_kinds matches every kind", withKinds(), ProducerEvent.newBuilder().setKind("anything").build(), true),
            Case("membership hit", withKinds("a", "b"), ProducerEvent.newBuilder().setKind("b").build(), true),
            Case("membership miss", withKinds("a", "b"), ProducerEvent.newBuilder().setKind("c").build(), false),
            Case("kinds are exact, not prefixes", withKinds("order"), ProducerEvent.newBuilder().setKind("order.charged").build(), false),
            Case("no EventMatch selects defensively", SpecificationFilter.newBuilder().build(), ProducerEvent.newBuilder().setKind("x").build(), true),
            Case("nil spec selects defensively", null, ProducerEvent.newBuilder().setKind("x").build(), true),
            Case("empty kind can still be matched", withKinds(""), ProducerEvent.newBuilder().build(), true),
            Case("empty kind against a non-empty set", withKinds("a"), ProducerEvent.newBuilder().build(), false),
        )
        for (c in cases) {
            assertEquals(c.want, SpecMatch.selects(c.spec, c.event), c.name)
        }
    }
}
