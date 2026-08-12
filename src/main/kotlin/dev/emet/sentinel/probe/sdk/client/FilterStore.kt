// Package client holds the Probe's connection to Sentinel and the versioned Event Filter for
// its source. Kotlin analog of sdk/go/client/.
//
// FilterStore is the concurrency primitive: a Probe emits from many threads while a filter push
// arrives on another, so the held filter pointer is swapped atomically. Go uses
// atomic.Pointer[T]; Kotlin's java.util.concurrent.atomic.AtomicReference<T> is the direct
// analog (a reference swap under volatile semantics), with the same read-then-write guarantee.
package dev.emet.sentinel.probe.sdk.client

import dev.emet.sentinel.model.v1.EventFilter
import java.util.concurrent.atomic.AtomicReference

// FilterStore holds the current EventFilter for the Probe's source and tracks the acknowledged
// epoch.
//
// Reference-swap semantics: set replaces the held reference atomically and get returns that
// stable reference. The store never mutates a held EventFilter in place, so a caller that
// snapshots the filter once for an emit-and-enforce flow holds a consistent view even if a set
// lands mid-flow. In JavaScript that property was free (single-threaded); in Kotlin it is a
// real concurrency requirement, and AtomicReference makes it an enforced invariant rather
// than a convention.
//
// The zero-constructed store is a usable, empty store. All methods are safe for concurrent use.
public class FilterStore internal constructor(initial: EventFilter?) {
    // AtomicReference<EventFilter?> is the direct analog of Go's atomic.Pointer[EventFilter].
    private val current: AtomicReference<EventFilter?> = AtomicReference(initial)

    public constructor() : this(null)

    // newFilterStore returns a store optionally seeded with an initial filter (for example one
    // restored from a local cache before the first push). A null initial filter means no filter
    // is held.
    public companion object {
        @JvmStatic
        public fun newFilterStore(initial: EventFilter? = null): FilterStore = FilterStore(initial)
    }

    // get returns the held EventFilter, or null before the first set. The returned reference is
    // stable until the next set: it is safe to hold it across an entire emit-and-enforce flow.
    public fun get(): EventFilter? = current.get()

    // epoch returns the held filter's epoch, or null when no filter is held or the held filter
    // declares no epoch. The result is a fresh value, so a caller cannot reach into the held
    // filter through it.
    //
    // null means "absent". It does not mean zero: epoch 0 is a legitimate epoch and is returned
    // as 0, not null. Presence is `hasEpoch`, never `getEpoch() == 0`.
    public fun epoch(): Long? {
        val held = current.get() ?: return null
        return if (held.hasEpoch()) held.epoch else null
    }

    // set swaps in a new filter. It reports whether the store was actually updated, which is
    // true when the epoch changed or when this is the first set, and false when an equal epoch
    // is re-pushed.
    //
    // Mirrors filter-store.ts:31-38, including the subtle case where the held filter and the new
    // filter both carry no epoch: that still counts as an update on the first set.
    public fun set(filter: EventFilter?): Boolean {
        val held = current.get()
        if (held != null && equalEpoch(epochOf(held), epochOf(filter))) {
            return false // epoch unchanged and not the first set -> no update
        }
        current.set(filter)
        return true
    }

    // shouldRefresh reports whether an announced epoch differs from the held one. No filter held
    // means refresh; no announced epoch means there is nothing to compare against, so do not
    // refresh.
    public fun shouldRefresh(newEpoch: Long?): Boolean {
        val held = epoch()
        if (held == null) return true // no filter held -> refresh
        if (newEpoch == null) return false // no new epoch to compare
        return held != newEpoch
    }

    override fun toString(): String = "FilterStore(epoch=${epoch()})"
}

// equalEpoch compares two optional epochs BY VALUE. Kotlin's `==` on Long? already compares by
// value (unlike Go's pointer comparison trap), but the helper is kept to mirror the reference's
// explicitness and to document that two distinct boxed Longs holding the same value compare
// equal here.
private fun equalEpoch(a: Long?, b: Long?): Boolean = a == b

// epochOf is null-safe on the message itself, unlike the generated getEpoch, which flattens an
// absent epoch to 0.
private fun epochOf(filter: EventFilter?): Long? = if (filter != null && filter.hasEpoch()) filter.epoch else null
