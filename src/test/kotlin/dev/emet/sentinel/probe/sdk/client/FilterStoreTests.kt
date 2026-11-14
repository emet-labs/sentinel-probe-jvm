package dev.emet.sentinel.probe.sdk.client

import dev.emet.sentinel.model.v1.EventFilter
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class FilterStoreTests {
    private fun makeFilter(epoch: Long?): EventFilter {
        val b = EventFilter.newBuilder()
        if (epoch != null) b.setEpoch(epoch)
        return b.build()
    }

    private fun u64(v: Long): Long = v

    @Test
    fun `get is null before first set`() {
        assertNull(FilterStore.newFilterStore().get())
    }

    @Test
    fun `zero-constructed store is usable`() {
        val store = FilterStore.newFilterStore()
        assertNull(store.get())
        assertTrue(store.set(makeFilter(u64(1L))))
        assertEquals(u64(1L), store.epoch())
    }

    @Test
    fun `first set swaps the pointer`() {
        val store = FilterStore.newFilterStore()
        assertTrue(store.set(makeFilter(u64(7L))))
        assertEquals(u64(7L), store.epoch())
    }

    @Test
    fun `unchanged epoch is a no-op`() {
        val store = FilterStore.newFilterStore(makeFilter(u64(7L)))
        assertFalse(store.set(makeFilter(u64(7L))))
    }

    @Test
    fun `set compares epochs by value not identity`() {
        // Two distinct EventFilter objects holding the same epoch must compare equal by value,
        // so the no-op branch fires. The Kotlin analog of the Go *uint64 pointer-comparison trap:
        // here equality is Long? == Long?, which compares by value — but the test pins it.
        val store = FilterStore.newFilterStore(makeFilter(u64(42L)))
        val freshAllocation = makeFilter(u64(42L))
        assertFalse(store.set(freshAllocation), "two distinct filters with the same epoch must be a no-op")
        assertTrue(store.get() !== freshAllocation, "the held pointer must not have been swapped")
    }

    @Test
    fun `changed epoch swaps`() {
        val store = FilterStore.newFilterStore(makeFilter(u64(1L)))
        assertTrue(store.set(makeFilter(u64(2L))))
        assertEquals(u64(2L), store.epoch())
    }

    @Test
    fun `epoch zero is present`() {
        // The epoch-0 trap: epoch 0 is a legitimate epoch, and presence is `hasEpoch`, never
        // `getEpoch() == 0`.
        val store = FilterStore.newFilterStore(makeFilter(u64(0L)))
        assertEquals(u64(0L), store.epoch(), "epoch 0 must be present, not null")
        assertFalse(store.set(makeFilter(u64(0L))), "re-pushing epoch 0 must be a no-op")
    }

    @Test
    fun `set on a filter without epoch`() {
        // The awkward corner of filter-store.ts:33: a held filter and a new filter that both
        // declare no epoch. The first set updates because the store was empty; the second does
        // not. Mirrors Go's TestFilterStoreSetOnFilterWithoutEpoch, which seeds with nil.
        val store = FilterStore.newFilterStore()
        assertTrue(store.set(makeFilter(null)), "first set must update even when the filter declares no epoch")
        assertNull(store.epoch(), "a filter without an epoch must report a null epoch")
        assertFalse(store.set(makeFilter(null)), "a second epochless set must be a no-op")
        assertTrue(store.set(makeFilter(u64(0L))), "moving from absent epoch to epoch 0 must be an update")
    }

    @Test
    fun `shouldRefresh with no filter held`() {
        val store = FilterStore.newFilterStore()
        assertTrue(store.shouldRefresh(u64(1L)))
        assertTrue(store.shouldRefresh(null))
    }

    @Test
    fun `shouldRefresh with no announced epoch`() {
        val store = FilterStore.newFilterStore(makeFilter(u64(1L)))
        assertFalse(store.shouldRefresh(null))
    }

    @Test
    fun `shouldRefresh compares epochs`() {
        val store = FilterStore.newFilterStore(makeFilter(u64(1L)))
        assertFalse(store.shouldRefresh(u64(1L)))
        assertTrue(store.shouldRefresh(u64(2L)))
    }

    @Test
    fun `snapshot survives a mid-flow set`() {
        // The reference-swap property: a caller that snapshots the filter for an emit-and-enforce
        // flow keeps a consistent view even when a push lands mid-flow.
        val store = FilterStore.newFilterStore(makeFilter(u64(1L)))
        val snapshot = store.get()
        store.set(makeFilter(u64(2L)))
        assertEquals(u64(1L), snapshot?.epoch, "the snapshot must keep the old epoch")
        assertEquals(u64(2L), store.epoch(), "the store must hold the new epoch")
    }

    @Test
    fun `concurrent get and set are safe`() {
        // The reason FilterStore uses AtomicReference at all. Go runs this under -race; here the
        // AtomicReference makes the swap atomic so no thread observes a torn pointer.
        val store = FilterStore.newFilterStore(makeFilter(u64(0L)))
        val latch = CountDownLatch(1)
        val writers =
            (1..8).map {
                Thread {
                    latch.await()
                    repeat(1000) { i -> store.set(makeFilter((i + 1L).toLong())) }
                }
            }
        val readers =
            (1..8).map {
                Thread {
                    latch.await()
                    repeat(2000) {
                        val epoch = store.epoch()
                        // epoch is either null or a non-negative value; never torn.
                        if (epoch != null) assertTrue(epoch >= 0L)
                        store.get()
                    }
                }
            }
        writers.forEach { it.start() }
        readers.forEach { it.start() }
        latch.countDown()
        writers.forEach { it.join() }
        readers.forEach { it.join() }
    }

    @Test
    fun `null and zero epochs are distinct`() {
        val store = FilterStore.newFilterStore(makeFilter(null))
        // Held no-epoch, new epoch present -> update.
        assertTrue(store.set(makeFilter(u64(0L))))
        // Held epoch 0, new no-epoch -> refused: an epoch-less filter never replaces an epoch.
        val held = store.get()
        assertFalse(store.set(makeFilter(null)))
        assertSame(held, store.get())
        assertEquals(0L, store.epoch())
    }

    @Test
    fun `first set applies whatever the epoch`() {
        for (epoch in listOf(7L, 0L, null)) {
            val store = FilterStore()
            val first = makeFilter(epoch)
            assertTrue(store.set(first))
            assertSame(first, store.get())
        }
    }

    @Test
    fun `a lower epoch is refused and the held filter kept`() {
        val store = FilterStore()
        val held = makeFilter(5L)
        store.set(held)
        assertFalse(store.set(makeFilter(4L)))
        assertFalse(store.set(makeFilter(0L)))
        assertSame(held, store.get())
        assertEquals(5L, store.epoch())
    }

    @Test
    fun `an epoch-less filter after an epoch-bearing one is refused`() {
        for (epoch in listOf(5L, 0L)) {
            val held = makeFilter(epoch)
            val store = FilterStore.newFilterStore(held)
            assertFalse(store.set(makeFilter(null)))
            assertFalse(store.set(null))
            assertSame(held, store.get())
        }
    }

    @Test
    fun `a higher epoch after epoch zero replaces`() {
        val store = FilterStore.newFilterStore(makeFilter(0L))
        val one = makeFilter(1L)
        assertTrue(store.set(one))
        assertSame(one, store.get())
    }

    @Test
    fun `epochs order as unsigned 64-bit values`() {
        // 2^63-1, 2^63 and 2^64-1 as the signed Longs that carry them.
        val u63Max = Long.MAX_VALUE
        val u63 = Long.MIN_VALUE
        val u64Max = -1L
        val store = FilterStore()
        assertTrue(store.set(makeFilter(u63Max)))
        assertTrue(store.set(makeFilter(u63)), "2^63 is newer than 2^63-1")
        val top = makeFilter(u64Max)
        assertTrue(store.set(top), "2^64-1 is newer than 2^63")
        assertFalse(store.set(makeFilter(u63)), "2^63 is older than 2^64-1")
        assertSame(top, store.get())
        val high = FilterStore.newFilterStore(makeFilter(u63))
        assertFalse(high.shouldRefresh(u63Max))
        assertTrue(high.shouldRefresh(u64Max))
    }

    @Test
    fun `a seeded store refuses a regression`() {
        val seed = makeFilter(9L)
        val store = FilterStore.newFilterStore(seed)
        assertFalse(store.set(makeFilter(8L)))
        assertSame(seed, store.get())
    }

    @Test
    fun `a refused set does not move later comparisons`() {
        val store = FilterStore.newFilterStore(makeFilter(5L))
        assertFalse(store.set(makeFilter(2L)))
        assertFalse(store.set(makeFilter(3L)))
        assertFalse(store.shouldRefresh(4L))
        val six = makeFilter(6L)
        assertTrue(store.set(six))
        assertSame(six, store.get())
    }

    @Test
    fun `shouldRefresh is true only for a strictly newer epoch`() {
        val store = FilterStore.newFilterStore(makeFilter(5L))
        assertFalse(store.shouldRefresh(4L))
        assertFalse(store.shouldRefresh(5L))
        assertTrue(store.shouldRefresh(6L))
        assertTrue(FilterStore.newFilterStore(makeFilter(null)).shouldRefresh(1L))
    }

    @Test
    fun `concurrent sets end at the highest epoch and never move backwards`() {
        val writers = 8
        val perWriter = 500
        val store = FilterStore()
        val start = CountDownLatch(1)
        val done = AtomicBoolean(false)
        val regression = AtomicReference<String?>(null)
        val observer =
            Thread {
                var last = -1L
                while (!done.get()) {
                    val epoch = store.epoch() ?: continue
                    if (epoch < last) {
                        regression.compareAndSet(null, "epoch went from $last to $epoch")
                        return@Thread
                    }
                    last = epoch
                }
            }
        val threads =
            (0 until writers).map { offset ->
                Thread {
                    start.await()
                    // Each writer walks its residue class downward, so writers keep offering
                    // epochs below the current maximum.
                    for (i in perWriter downTo 1) store.set(makeFilter((i - 1L) * writers + offset + 1))
                }
            }
        observer.start()
        threads.forEach { it.start() }
        start.countDown()
        threads.forEach { it.join() }
        done.set(true)
        observer.join()
        assertNull(regression.get())
        assertEquals((writers * perWriter).toLong(), store.epoch())
    }
}
