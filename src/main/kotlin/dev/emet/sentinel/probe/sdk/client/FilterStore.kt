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
public class FilterStore internal constructor(
    initial: EventFilter?,
) {
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

    // set swaps in a new filter under the epoch order, reporting whether the store was actually
    // updated. Epochs are unsigned 64-bit and only move forward (ADR-0006). With held the held
    // filter's epoch and new the offered one's, either of which may be absent:
    //
    //   - Nothing held: any filter applies, including epoch 0 and an epoch-less one.
    //   - Held is absent: a filter with an epoch (including 0) replaces it; an epoch-less one is
    //     a no-op.
    //   - Held is present: only a strictly greater epoch replaces it. An equal epoch is a no-op,
    //     the ordinary at-least-once re-delivery. A lower or absent epoch is refused, so a
    //     reordered or replayed push never downgrades the filter or the acknowledged epoch.
    //
    // A set that returns false keeps the held reference and changes no later comparison; it
    // throws nothing and logs nothing. A null filter counts as epoch-less.
    //
    // An equal epoch is never applied, even when its content differs. The server rewrites the
    // current epoch in place when its Readiness flips Warming to Active, so whether a Probe
    // should accept that flip at an equal epoch is an open question (#534); the store
    // keeps its earlier behaviour until it is decided. After a server store reset, epochs
    // restart from 1 and the store refuses them until the Probe restarts; restarting is the
    // recovery.
    //
    // Mirrors filter-store.ts. The proto field is uint64 and Kotlin carries it in a signed Long,
    // so epochs compare with java.lang.Long.compareUnsigned: 2^63 and above sort above 2^63-1.
    // The compare-and-set loop keeps the order under concurrent pushes.
    public fun set(filter: EventFilter?): Boolean {
        val offered = epochOf(filter)
        while (true) {
            val held = current.get()
            if (held != null && !replaces(epochOf(held), offered)) {
                return false // equal, older, or epoch-less: keep the held filter
            }
            if (current.compareAndSet(held, filter)) return true
        }
    }

    // shouldRefresh reports whether an announced epoch is strictly newer (unsigned) than the
    // held one. No epoch held means refresh; no announced epoch means there is nothing to compare
    // against, so do not refresh.
    public fun shouldRefresh(newEpoch: Long?): Boolean {
        val held = epoch()
        if (held == null) return true // no epoch held -> refresh
        if (newEpoch == null) return false // no new epoch to compare
        return java.lang.Long.compareUnsigned(newEpoch, held) > 0
    }

    override fun toString(): String = "FilterStore(epoch=${epoch()})"
}

// replaces reports whether a filter at offered replaces one held at held: an absent offered
// epoch never does, any offered epoch replaces an absent held one, and otherwise only a strictly
// greater epoch in unsigned order does.
internal fun replaces(
    held: Long?,
    offered: Long?,
): Boolean {
    if (offered == null) return false
    if (held == null) return true
    return java.lang.Long.compareUnsigned(offered, held) > 0
}

// epochOf is null-safe on the message itself, unlike the generated getEpoch, which flattens an
// absent epoch to 0.
private fun epochOf(filter: EventFilter?): Long? = if (filter != null && filter.hasEpoch()) filter.epoch else null
