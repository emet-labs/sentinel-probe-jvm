// Package filter implements attribute-level Event Filter projection (ADR-0006).
// Kotlin analog of sdk/go/filter/apply.go and sdk/typescript/src/filter/apply-filter.ts.
//
// This is RELEVANCE PROJECTION, not sampling: it never drops a relevant event, never drops an
// attribute any selecting Specification could need, and never invents data. Where the answer is
// uncertain it over-approximates upward — keeping more than strictly necessary is sound;
// keeping less is not.
package dev.emet.sentinel.probe.sdk.filter

import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SequenceCoordinate
import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.probe.sdk.internal.specmatch.SpecMatch

public object ApplyFilter {
    // apply projects event against filter. It returns a possibly attribute-trimmed ProducerEvent
    // when at least one SpecificationFilter's EventMatch selects the event, and null when none
    // does, meaning the event is irrelevant to every Specification and can be dropped entirely.
    //
    // The algorithm mirrors apply-filter.ts:19-70 step for step:
    //
    //  1. collect the SpecificationFilters whose EventMatch selects the event;
    //  2. none selecting means drop, sound because no Specification depends on the event;
    //  3. if ANY selecting spec has an empty projected_attribute_keys, keep every attribute
    //     (over-approximate upward: that spec might need any of them);
    //  4. otherwise keep the union of the selecting specs' projected keys;
    //  5. rebuild the event with every other field unchanged. causal_predecessor_ids is NEVER
    //     trimmed — it is the causal skeleton, not an attribute.
    //
    // A null filter behaves as a filter with no specifications, i.e. drop.
    public fun apply(event: ProducerEvent?, filter: EventFilter?): ProducerEvent? {
        if (event == null) return null

        // 1. Collect the SpecificationFilters whose EventMatch selects the event.
        val selecting = (filter?.specificationsList ?: emptyList()).filter { SpecMatch.selects(it, event) }

        // 2. No spec selects: drop entirely.
        if (selecting.isEmpty()) return null

        // 3. Any selecting spec with an empty projection set means keep everything.
        val projectAll = selecting.any { it.eventMatch.projectedAttributeKeysList.isEmpty() }

        // 4. Otherwise keep the union of projected keys.
        val sourceAttrs = event.attributesList
        val trimmed: List<AttributeEntry> = if (projectAll) {
            // Fresh list: the returned event never aliases the input's attribute collection
            // (the aliasing contract documented in the Go reference).
            ArrayList(sourceAttrs)
        } else {
            val projected = HashSet<String>()
            for (spec in selecting) {
                projected.addAll(spec.eventMatch.projectedAttributeKeysList)
            }
            val out = ArrayList<AttributeEntry>(sourceAttrs.size)
            for (entry in sourceAttrs) {
                if (entry.key in projected) out.add(entry)
            }
            out
        }

        // 5. Rebuild with every other field unchanged.
        val builder = ProducerEvent.newBuilder()
            .setId(event.id)
            .setSchemaVersion(event.schemaVersion)
            .setKind(event.kind)
            .addAllAttributes(trimmed)
            .addAllCausalPredecessorIds(event.causalPredecessorIdsList)
            .setClaimedSensitivity(event.claimedSensitivity)
        // Optional/sub-message fields are copied only when present, so a null sequence or
        // occurrence time is not replaced by a default instance (proto3 message presence).
        if (event.hasSequence()) builder.setSequence(event.sequence)
        if (event.hasAcknowledgedFilterEpoch()) {
            builder.setAcknowledgedFilterEpoch(event.acknowledgedFilterEpoch)
        }
        if (event.hasOccurrenceTime()) builder.setOccurrenceTime(event.occurrenceTime)
        if (event.claimedCapabilitiesCount > 0) {
            builder.addAllClaimedCapabilities(event.claimedCapabilitiesList)
        }
        return builder.build()
    }
}
