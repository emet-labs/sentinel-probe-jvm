// ParentEventId carries a caller's current Event id across a process boundary, so the callee's
// Probe can put it in sentinel.parent.event.id.
//
// The id travels in the x-sentinel-parent-event-id header next to traceparent, on HTTP and on
// Kafka alike. The sentinel member of the W3C tracestate header is the fallback when extracting.
// The helpers are carrier-agnostic: pass a getter or setter for any header container, or use the
// Map overloads. A caller that is not a Probe sends neither, and the callee's Event is exported
// with no edge to it (see ExportOptions.allowUnattributedRemoteParent).
package dev.emet.sentinel.probe.sdk.emission

// PARENT_EVENT_ID_HEADER is the propagation header. Header names are case-insensitive on the
// wire; the Map overloads match them that way.
public const val PARENT_EVENT_ID_HEADER: String = "x-sentinel-parent-event-id"

// TRACESTATE_HEADER and TRACESTATE_SENTINEL_MEMBER name the extract fallback: tracestate
// `sentinel=<eventId>`.
public const val TRACESTATE_HEADER: String = "tracestate"
public const val TRACESTATE_SENTINEL_MEMBER: String = "sentinel"

public object ParentEventIds {
    // inject writes eventId to the carrier through setter. It returns false and writes nothing
    // when eventId is blank or holds a control character (a header-injection hazard), so a host
    // can call it on the request path without a guard.
    @JvmStatic
    public fun <C> inject(
        carrier: C,
        eventId: String?,
        setter: (carrier: C, name: String, value: String) -> Unit,
    ): Boolean {
        if (eventId == null || !isPropagatable(eventId)) return false
        setter(carrier, PARENT_EVENT_ID_HEADER, eventId)
        return true
    }

    // extract reads the caller's Event id through getter: the x-sentinel-parent-event-id header
    // first, then the tracestate sentinel member. It returns null when neither carries a usable
    // id. The getter receives lower-case header names and returns the header's value or null.
    @JvmStatic
    public fun <C> extract(
        carrier: C,
        getter: (carrier: C, name: String) -> String?,
    ): String? {
        val header = getter(carrier, PARENT_EVENT_ID_HEADER)?.trim()
        if (header != null && isPropagatable(header)) return header
        val tracestate = getter(carrier, TRACESTATE_HEADER) ?: return null
        return sentinelMember(tracestate)
    }

    // sentinelMember finds `sentinel=<id>` among the comma-separated tracestate members.
    private fun sentinelMember(tracestate: String): String? {
        for (member in tracestate.split(',')) {
            val trimmed = member.trim()
            val separator = trimmed.indexOf('=')
            if (separator <= 0) continue
            if (trimmed.substring(0, separator).trim() != TRACESTATE_SENTINEL_MEMBER) continue
            val value = trimmed.substring(separator + 1).trim()
            if (isPropagatable(value)) return value
        }
        return null
    }

    private fun isPropagatable(eventId: String): Boolean = eventId.isNotBlank() && eventId.none { it.isISOControl() }
}

// injectParentEventId sets x-sentinel-parent-event-id on a mutable header map. Any existing entry
// that differs only in case is replaced. It returns false when eventId cannot be propagated.
public fun injectParentEventId(
    carrier: MutableMap<String, String>,
    eventId: String?,
): Boolean =
    ParentEventIds.inject(carrier, eventId) { map, name, value ->
        map.keys.removeIf { it.equals(name, ignoreCase = true) }
        map[name] = value
    }

// injectParentEventId is the setter form, for a carrier that is not a Map (an HTTP request
// builder, Kafka's Headers).
public fun <C> injectParentEventId(
    carrier: C,
    eventId: String?,
    setter: (carrier: C, name: String, value: String) -> Unit,
): Boolean = ParentEventIds.inject(carrier, eventId, setter)

// extractParentEventId reads the caller's Event id from a header map, matching names
// case-insensitively. Returns null when neither the header nor the tracestate member is usable.
public fun extractParentEventId(carrier: Map<String, String>): String? =
    ParentEventIds.extract(carrier) { map, name ->
        map.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value
    }

// extractParentEventId is the getter form, for a carrier that is not a Map.
public fun <C> extractParentEventId(
    carrier: C,
    getter: (carrier: C, name: String) -> String?,
): String? = ParentEventIds.extract(carrier, getter)
