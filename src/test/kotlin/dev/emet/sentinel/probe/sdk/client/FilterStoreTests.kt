package dev.emet.sentinel.probe.sdk.client

import dev.emet.sentinel.model.v1.EventFilter
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
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
        val writers = (1..8).map {
            Thread {
                latch.await()
                repeat(1000) { i -> store.set(makeFilter((i + 1L).toLong())) }
            }
        }
        val readers = (1..8).map {
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
    fun `equalEpoch semantics - null and zero are distinct`() {
        val store = FilterStore.newFilterStore(makeFilter(null))
        // Held no-epoch, new epoch present -> update.
        assertTrue(store.set(makeFilter(u64(0L))))
        // Held epoch 0, new no-epoch -> update.
        assertTrue(store.set(makeFilter(null)))
        assertNull(store.epoch())
    }
}
