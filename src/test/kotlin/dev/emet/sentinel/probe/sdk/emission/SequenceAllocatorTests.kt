package dev.emet.sentinel.probe.sdk.emission

import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SequenceAllocatorTests {
    private val startMillis = 1_794_000_000_000L
    private val clock = AtomicLong(startMillis * NANOS_PER_MILLI)

    private fun allocator(
        epoch: Long? = null,
        previousEpoch: Long? = null,
    ): SequenceAllocator = SequenceAllocator(nowUnixNanos = clock::get, epoch = epoch, previousEpoch = previousEpoch)

    private fun setClockMillis(millis: Long) = clock.set(millis * NANOS_PER_MILLI)

    @Test
    fun `the epoch is the clock in Unix milliseconds`() {
        clock.set(startMillis * NANOS_PER_MILLI + 999_999L)
        val sequence = allocator()
        assertEquals(startMillis, sequence.epoch())
        assertEquals(Slot(startMillis, 1L), sequence.allocate())
    }

    @Test
    fun `an explicit epoch overrides the clock`() {
        assertEquals(Slot(42L, 1L), allocator(epoch = 42L).allocate())
    }

    @Test
    fun `a previous epoch the clock is ahead of keeps the clock`() {
        assertEquals(startMillis, allocator(previousEpoch = startMillis - 60_000L).epoch())
    }

    @Test
    fun `a previous epoch the clock is behind takes previous plus one`() {
        assertEquals(startMillis + 1L, allocator(previousEpoch = startMillis).epoch())
        assertEquals(startMillis + 60_001L, allocator(previousEpoch = startMillis + 60_000L).epoch())
    }

    @Test
    fun `an override that is not greater than the previous epoch is refused`() {
        val equal = assertFailsWith<SequenceException> { allocator(epoch = 100L, previousEpoch = 100L) }
        assertEquals(SequenceException.EPOCH_NOT_GREATER, equal.code)
        val lower = assertFailsWith<SequenceException> { allocator(epoch = 50L, previousEpoch = 100L) }
        assertEquals(SequenceException.EPOCH_NOT_GREATER, lower.code)
        assertEquals(101L, allocator(epoch = 101L, previousEpoch = 100L).epoch())
    }

    @Test
    fun `negative epochs are refused at construction`() {
        assertFailsWith<IllegalArgumentException> { allocator(epoch = -1L) }
        assertFailsWith<IllegalArgumentException> { allocator(previousEpoch = -1L) }
    }

    @Test
    fun `a fresh epoch declares zero with no filtered range and no previous epoch`() {
        val declaration = allocator().declaration()
        assertEquals(SequenceDeclaration(startMillis, 0L, 0L, emptyList(), null), declaration)
        assertEquals(0L, allocator().highestAllocated())
    }

    @Test
    fun `allocation numbers slots from one within the epoch`() {
        val sequence = allocator()
        assertEquals(listOf(1L, 2L, 3L), List(3) { sequence.allocate().number })
        assertEquals(3L, sequence.highestAllocated())
        assertEquals(EpochDeclaration(startMillis, 3L, 0L), sequence.declaration().epochDeclaration())
    }

    @Test
    fun `complete through advances only over a whole exported prefix`() {
        val sequence = allocator()
        val slots = List(3) { sequence.allocate() }
        sequence.markExported(slots[2])
        assertEquals(0L, sequence.declaration().completeThrough)
        sequence.markExported(slots[0])
        assertEquals(1L, sequence.declaration().completeThrough)
        sequence.markExported(slots[1])
        assertEquals(3L, sequence.declaration().completeThrough)
    }

    @Test
    fun `a failed export stalls the prefix until the slot is exported`() {
        val sequence = allocator()
        val slots = List(3) { sequence.allocate() }
        sequence.markExported(slots[0])
        sequence.markFailed(slots[1])
        sequence.markExported(slots[2])
        assertEquals(1L, sequence.declaration().completeThrough)
        sequence.markExported(slots[1])
        assertEquals(3L, sequence.declaration().completeThrough)
    }

    @Test
    fun `a filtered slot stalls the prefix and records its Filter epoch`() {
        val sequence = allocator()
        val slots = List(3) { sequence.allocate() }
        sequence.markExported(slots[0])
        sequence.markFiltered(slots[1], 4L)
        sequence.markExported(slots[2])
        val declaration = sequence.declaration()
        assertEquals(1L, declaration.completeThrough)
        assertEquals(listOf(FilteredRange(2L, 2L, 4L)), declaration.filtered)
        sequence.markExported(slots[1])
        assertEquals(1L, sequence.declaration().completeThrough, "FILTERED is terminal until #542")
    }

    @Test
    fun `adjacent filtered slots merge only within one Filter epoch`() {
        val sequence = allocator()
        val slots = List(7) { sequence.allocate() }
        sequence.markFiltered(slots[3], 4L)
        sequence.markFiltered(slots[1], 4L)
        sequence.markFiltered(slots[2], 4L)
        sequence.markFiltered(slots[4], 5L)
        sequence.markFiltered(slots[6], 5L)
        assertEquals(
            listOf(FilteredRange(2L, 4L, 4L), FilteredRange(5L, 5L, 5L), FilteredRange(7L, 7L, 5L)),
            sequence.declaration().filtered,
        )
    }

    @Test
    fun `marks for unknown, foreign or settled slots are no-ops`() {
        val sequence = allocator()
        val first = sequence.allocate()
        val second = sequence.allocate()
        sequence.markExported(Slot(first.epoch + 1L, 1L))
        sequence.markExported(Slot(first.epoch, 3L))
        sequence.markExported(Slot(first.epoch, 0L))
        assertEquals(0L, sequence.declaration().completeThrough)
        sequence.markExported(first)
        sequence.markFailed(first)
        sequence.markFiltered(first, 1L)
        assertEquals(1L, sequence.declaration().completeThrough)
        assertEquals(emptyList(), sequence.declaration().filtered)
        sequence.markFiltered(second, 2L)
        sequence.markFailed(second)
        sequence.markExported(second)
        assertEquals(1L, sequence.declaration().completeThrough)
        assertEquals(listOf(FilteredRange(2L, 2L, 2L)), sequence.declaration().filtered)
    }

    @Test
    fun `a failed slot cannot be marked filtered`() {
        val sequence = allocator()
        val slot = sequence.allocate()
        sequence.markFailed(slot)
        sequence.markFiltered(slot, 1L)
        assertEquals(emptyList(), sequence.declaration().filtered)
    }

    @Test
    fun `declaration never seals and the next allocation stays in the epoch`() {
        val sequence = allocator()
        sequence.markExported(sequence.allocate())
        sequence.declaration()
        assertEquals(Slot(startMillis, 2L), sequence.allocate())
    }

    @Test
    fun `declare with a slot in flight does not seal`() {
        val sequence = allocator()
        sequence.allocate()
        assertEquals(EpochDeclaration(startMillis, 1L, 0L), sequence.declare().epochDeclaration())
        assertEquals(Slot(startMillis, 2L), sequence.allocate())
    }

    @Test
    fun `a declared close seals the epoch and the next allocation rolls`() {
        val sequence = allocator()
        sequence.markExported(sequence.allocate())
        assertEquals(EpochDeclaration(startMillis, 1L, 1L), sequence.declare().epochDeclaration())
        assertEquals(Slot(startMillis + 1L, 1L), sequence.allocate())
        val rolled = sequence.declaration()
        assertEquals(EpochDeclaration(startMillis, 1L, 1L), rolled.previous)
        assertEquals(EpochDeclaration(startMillis + 1L, 1L, 0L), rolled.epochDeclaration())
    }

    @Test
    fun `the roll takes the clock when it is ahead of the next epoch`() {
        val sequence = allocator()
        sequence.markExported(sequence.allocate())
        sequence.declare()
        setClockMillis(startMillis + 60_000L)
        assertEquals(Slot(startMillis + 60_000L, 1L), sequence.allocate())
    }

    @Test
    fun `an empty epoch declared closed rolls on the next allocation`() {
        val sequence = allocator()
        assertEquals(EpochDeclaration(startMillis, 0L, 0L), sequence.declare().epochDeclaration())
        assertEquals(Slot(startMillis + 1L, 1L), sequence.allocate())
        assertEquals(EpochDeclaration(startMillis, 0L, 0L), sequence.declaration().previous)
    }

    @Test
    fun `after the roll a mark for the closed epoch is a no-op`() {
        val sequence = allocator()
        assertNull(sequence.declaration().previous)
        val slot = sequence.allocate()
        sequence.markFailed(slot)
        sequence.markExported(slot)
        sequence.declare()
        val next = sequence.allocate()
        sequence.markFiltered(next, 2L)
        assertEquals(listOf(FilteredRange(1L, 1L, 2L)), sequence.declaration().filtered)
        sequence.markExported(slot)
        assertEquals(EpochDeclaration(startMillis + 1L, 1L, 0L), sequence.declaration().epochDeclaration())
        assertEquals(EpochDeclaration(startMillis, 1L, 1L), sequence.declaration().previous)
    }

    @Test
    fun `an epoch past the maximum is an overflow, never a wrap`() {
        val rolled = allocator(epoch = Long.MAX_VALUE)
        rolled.declare()
        val roll = assertFailsWith<SequenceException> { rolled.allocate() }
        assertEquals(SequenceException.SEQUENCE_OVERFLOW, roll.code)
        assertEquals(Long.MAX_VALUE, rolled.epoch())
        val restart = assertFailsWith<SequenceException> { allocator(previousEpoch = Long.MAX_VALUE) }
        assertEquals(SequenceException.SEQUENCE_OVERFLOW, restart.code)
    }

    @Test
    fun `no slot is ever allocated in an epoch whose close was declared`() {
        // I3: allocation and declare() race on the one lock. Either declare() sees the slot in
        // flight, or the allocation opens a new epoch; a slot never lands in a closed epoch.
        val sequence = allocator()
        val workers = 4
        val perWorker = 2_000
        val slots = ConcurrentHashMap.newKeySet<Slot>()
        val closed = ConcurrentHashMap<Long, Long>()
        val done = AtomicBoolean(false)
        val ready = CountDownLatch(workers + 1)
        val declarer =
            thread {
                ready.countDown()
                ready.await()
                while (!done.get()) {
                    val declaration = sequence.declare()
                    if (declaration.completeThrough == declaration.lastEmitted) {
                        closed[declaration.epoch] = declaration.lastEmitted
                    }
                }
            }
        val threads =
            List(workers) {
                thread {
                    ready.countDown()
                    ready.await()
                    repeat(perWorker) {
                        val slot = sequence.allocate()
                        assertTrue(slots.add(slot), "slot $slot allocated twice")
                        sequence.markExported(slot)
                    }
                }
            }
        threads.forEach { it.join() }
        done.set(true)
        declarer.join()
        assertEquals(workers * perWorker, slots.size)
        for (slot in slots) {
            val close = closed[slot.epoch] ?: continue
            assertTrue(slot.number <= close, "slot $slot was allocated after epoch ${slot.epoch} closed at $close")
        }
    }

    private fun SequenceDeclaration.epochDeclaration(): EpochDeclaration = EpochDeclaration(epoch, lastEmitted, completeThrough)

    private companion object {
        const val NANOS_PER_MILLI = 1_000_000L
    }
}
