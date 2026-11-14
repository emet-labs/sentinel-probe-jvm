// SequenceAllocator numbers a source's Events and tracks which numbers Sentinel holds (#461).
//
// One allocator serves one source. allocate() hands out (Sequence Epoch, number) slots, and the
// exporter marks each slot exported, failed or filtered. The completeness heartbeat reads the
// declaration: last_emitted is the highest number allocated, and complete_through is the
// contiguous prefix whose export succeeded. complete_through never passes a slot that is in
// flight, failed or filtered, so missing evidence is never declared complete.
//
// A declared close (complete_through == last_emitted, returned by declare()) seals the epoch:
// Sentinel refuses any later slot in it. The next allocation therefore opens a new epoch,
// max(epoch + 1, clock in Unix milliseconds). The roll is lazy, at allocation and never at the
// heartbeat, so a quiet source keeps its epoch.
//
// Epochs and numbers are non-negative Longs, at most 2^63 - 1. Allocation past the maximum is
// a SequenceException, never a wrap. One lock guards all state, so every method is safe to
// call from any thread.
package dev.emet.sentinel.probe.sdk.emission

import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

// Slot is one allocated sequence position: the Sequence Epoch and the number within it.
public data class Slot(
    val epoch: Long,
    val number: Long,
)

// FilteredRange is a run of slots the Event Filter dropped under one Filter epoch. It is not
// on the wire until #542; the declaration carries it so that change stays small.
public data class FilteredRange(
    val first: Long,
    val last: Long,
    val filterEpoch: Long,
)

// EpochDeclaration is the final declaration of one Sequence Epoch.
public data class EpochDeclaration(
    val epoch: Long,
    val lastEmitted: Long,
    val completeThrough: Long,
)

// SequenceDeclaration is what the heartbeat reports: the current epoch's counters, its filtered
// ranges (sorted, merged within one Filter epoch), and the final declaration of the epoch closed
// before it, if any.
public data class SequenceDeclaration(
    val epoch: Long,
    val lastEmitted: Long,
    val completeThrough: Long,
    val filtered: List<FilteredRange>,
    val previous: EpochDeclaration?,
)

// SequenceException carries a stable error code shared with every Probe SDK and the
// sequence_completeness vectors.
public class SequenceException(
    public val code: String,
    message: String,
) : IllegalStateException(message) {
    public companion object {
        // An explicit epoch that is not strictly greater than the previous run's epoch.
        public const val EPOCH_NOT_GREATER: String = "epoch-not-greater"

        // An epoch or a number past 2^63 - 1.
        public const val SEQUENCE_OVERFLOW: String = "sequence-overflow"

        // The acknowledged Filter epoch went below one already sent for the epoch.
        public const val FILTER_EPOCH_REGRESSED: String = "filter-epoch-regressed"
    }
}

@Suppress("TooManyFunctions")
public class SequenceAllocator(
    // nowUnixNanos is the clock. The epoch is its Unix milliseconds.
    private val nowUnixNanos: () -> Long = ::systemUnixNanos,
    // epoch overrides the clock-derived first epoch.
    epoch: Long? = null,
    // previousEpoch is the last epoch of the previous run, when the host persisted it. Without
    // it a backward clock step across a restart goes undetected, and Sentinel refuses the
    // regressed epoch.
    previousEpoch: Long? = null,
) {
    private enum class SlotState { IN_FLIGHT, EXPORTED, FAILED, FILTERED }

    private val lock = ReentrantLock()
    private var current: Long
    private var last = 0L
    private var completeThrough = 0L
    private var sealed = false
    private val states = HashMap<Long, SlotState>()
    private var filtered: List<FilteredRange> = emptyList()
    private var previous: EpochDeclaration? = null

    init {
        require(epoch == null || epoch >= 0) { "epoch must be zero or positive" }
        require(previousEpoch == null || previousEpoch >= 0) { "previous epoch must be zero or positive" }
        current =
            when {
                epoch != null && previousEpoch != null && epoch <= previousEpoch ->
                    throw SequenceException(
                        SequenceException.EPOCH_NOT_GREATER,
                        "epoch $epoch is not greater than the previous epoch $previousEpoch",
                    )
                epoch != null -> epoch
                previousEpoch != null -> maxOf(clockMillis(), successor(previousEpoch))
                else -> clockMillis()
            }
    }

    // allocate returns the next slot. After a declared close it first opens a new epoch.
    public fun allocate(): Slot =
        lock.withLock {
            if (sealed) {
                val opened = maxOf(successor(current), clockMillis())
                previous = EpochDeclaration(current, last, completeThrough)
                current = opened
                last = 0L
                completeThrough = 0L
                states.clear()
                filtered = emptyList()
                sealed = false
            }
            last = successor(last)
            states[last] = SlotState.IN_FLIGHT
            Slot(current, last)
        }

    // markExported records that Sentinel accepted the slot's Event (HTTP 2xx, zero rejected).
    // A failed slot the host re-exported may be marked exported.
    public fun markExported(slot: Slot) {
        lock.withLock {
            val state = stateOf(slot) ?: return
            if (state != SlotState.IN_FLIGHT && state != SlotState.FAILED) return
            states[slot.number] = SlotState.EXPORTED
            while (states[completeThrough + 1] == SlotState.EXPORTED) {
                states.remove(completeThrough + 1)
                completeThrough += 1
            }
        }
    }

    // markFailed records that the slot's export failed. The helper does not retry it; the epoch
    // stays open below the slot until the host re-exports the identical Event.
    public fun markFailed(slot: Slot) {
        lock.withLock {
            if (stateOf(slot) == SlotState.IN_FLIGHT) states[slot.number] = SlotState.FAILED
        }
    }

    // markFiltered records that the Event Filter dropped the slot's Event under filterEpoch.
    // Until #542 a filtered slot does not advance complete_through.
    public fun markFiltered(
        slot: Slot,
        filterEpoch: Long,
    ) {
        lock.withLock {
            if (stateOf(slot) != SlotState.IN_FLIGHT) return
            states[slot.number] = SlotState.FILTERED
            filtered = mergeFiltered(filtered + FilteredRange(slot.number, slot.number, filterEpoch))
        }
    }

    // epoch is the current Sequence Epoch.
    public fun epoch(): Long = lock.withLock { current }

    // highestAllocated is the highest number allocated in the current epoch, 0 before any.
    public fun highestAllocated(): Long = lock.withLock { last }

    // declaration reads the current declaration without sealing anything.
    public fun declaration(): SequenceDeclaration = lock.withLock { snapshot() }

    // declare returns the declaration the heartbeat sends. A close (complete_through ==
    // last_emitted) seals the epoch, so no slot is ever allocated in it afterwards.
    public fun declare(): SequenceDeclaration =
        lock.withLock {
            if (completeThrough == last) sealed = true
            snapshot()
        }

    private fun snapshot(): SequenceDeclaration = SequenceDeclaration(current, last, completeThrough, filtered, previous)

    // stateOf is null for a slot of another epoch, a number never allocated, or a number at or
    // below complete_through (EXPORTED, already discarded).
    private fun stateOf(slot: Slot): SlotState? {
        if (slot.epoch != current || slot.number < 1 || slot.number > last) return null
        return states[slot.number]
    }

    private fun clockMillis(): Long {
        val millis = Math.floorDiv(nowUnixNanos(), NANOS_PER_MILLI)
        check(millis >= 0) { "clock is before the Unix epoch" }
        return millis
    }

    private companion object {
        const val NANOS_PER_MILLI = 1_000_000L
    }
}

// successor is value + 1, or a SEQUENCE_OVERFLOW past 2^63 - 1. It never wraps.
private fun successor(value: Long): Long {
    if (value == Long.MAX_VALUE) {
        throw SequenceException(SequenceException.SEQUENCE_OVERFLOW, "sequence value $value has no successor")
    }
    return value + 1
}

// mergeFiltered sorts the ranges and merges adjacent ones that share a Filter epoch. Slots are
// filtered at most once, so the ranges never overlap.
private fun mergeFiltered(ranges: List<FilteredRange>): List<FilteredRange> {
    val merged = ArrayList<FilteredRange>()
    for (range in ranges.sortedBy { it.first }) {
        val tail = merged.lastOrNull()
        if (tail != null && tail.filterEpoch == range.filterEpoch && tail.last + 1 == range.first) {
            merged[merged.size - 1] = tail.copy(last = range.last)
        } else {
            merged.add(range)
        }
    }
    return merged
}

// systemUnixNanos reads the wall clock in Unix nanoseconds. Only production defaults use it;
// tests inject a clock.
internal fun systemUnixNanos(): Long {
    val now = Instant.now()
    return Math.addExact(Math.multiplyExact(now.epochSecond, 1_000_000_000L), now.nano.toLong())
}
