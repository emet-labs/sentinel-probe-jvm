// Package specmatch holds the single definition of "does this SpecificationFilter select this
// ProducerEvent", shared by the filter and enforcement packages.
//
// It lives under internal/ deliberately. In the TypeScript reference, specSelects is exported
// from apply-filter.ts so the enforcement gate can reuse it — that reuse was a review fix, to
// stop the gate carrying a second, drifting copy of the predicate — but it is NOT re-exported
// from src/index.ts, so it is not public SDK API. Kotlin has no module-internal export, so the
// internal/ package + internal visibility reproduces the reference's hidden surface.
package dev.emet.sentinel.probe.sdk.internal.specmatch

import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter

internal object SpecMatch {
    // selects reports whether spec's EventMatch selects event: its event_kinds is empty, or it
    // includes event.kind. A spec with no EventMatch at all selects everything, defensively —
    // the same over-approximate-upward choice ADR-0006 requires of the projection itself.
    //
    // The generated null-safe getters give this the optional-chaining semantics of
    // apply-filter.ts:77-81 for free: a null spec is treated as "selects everything" (a null
    // EventMatch yields an empty event_kinds list), and a null event yields an empty kind.
    internal fun selects(
        spec: SpecificationFilter?,
        event: ProducerEvent?,
    ): Boolean {
        val kinds = spec?.eventMatch?.eventKindsList ?: emptyList()
        if (kinds.isEmpty()) return true
        return kinds.contains(event?.kind ?: "")
    }
}
