package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.SourceActivityState
import dev.emet.sentinel.probe.sdk.emission.EpochDeclaration
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.SequenceException
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import org.junit.jupiter.api.Test
import java.util.Collections
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Delayed
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CompletenessHeartbeatTests {
    private val startNanos = 1_794_000_000_000L * 1_000_000L
    private val clock = AtomicLong(startNanos)
    private val sent: MutableList<ProducerCompletenessSignal> = Collections.synchronizedList(ArrayList())
    private val errors: MutableList<Throwable> = Collections.synchronizedList(ArrayList())
    private val sequence = SequenceAllocator(nowUnixNanos = clock::get)

    private fun heartbeat(
        send: (ProducerCompletenessSignal) -> Unit = { sent.add(it) },
        filterEpoch: () -> Long? = { null },
        intervalMillis: Long = 0L,
        scheduler: ScheduledExecutorService? = null,
    ): CompletenessHeartbeat =
        CompletenessHeartbeat(
            sequence = sequence,
            send = send,
            filterEpoch = filterEpoch,
            nowUnixNanos = clock::get,
            intervalMillis = intervalMillis,
            scheduler = scheduler,
            onError = { errors.add(it) },
        )

    @Test
    fun `the encoded signal sets presence on zero counters and a zero Filter epoch`() {
        val reports = heartbeat().report()
        assertEquals(listOf(CompletenessReport(sequence.epoch(), 0L, 0L, 0L, true)), reports)
        val signal = ProducerCompletenessSignal.parseFrom(sent.single().toByteArray())
        assertTrue(signal.hasLastEmitted())
        assertTrue(signal.hasCompleteThrough())
        assertTrue(signal.hasAcknowledgedFilterEpoch())
        assertEquals(0L, signal.lastEmitted)
        assertEquals(0L, signal.completeThrough)
        assertEquals(0L, signal.acknowledgedFilterEpoch)
        assertEquals(SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE, signal.activity)
        for (time in listOf(signal.producerReportedAt, signal.activitySince)) {
            assertEquals("unix", time.clockDomainId)
            assertEquals(0L, time.uncertaintyNanoseconds)
            assertEquals(startNanos.toBigInteger(), Int128Codec.toBigInt(time.nanoseconds))
        }
    }

    @Test
    fun `build sets every field the restatement check compares`() {
        val signal =
            CompletenessSignals.build(
                EpochDeclaration(7L, 3L, 1L),
                acknowledgedFilterEpoch = 2L,
                activity = SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE,
                reportedAtUnixNanos = 20L,
                activitySinceUnixNanos = 10L,
            )
        assertEquals(7L, signal.sequenceEpoch)
        assertEquals(3L, signal.lastEmitted)
        assertEquals(1L, signal.completeThrough)
        assertEquals(2L, signal.acknowledgedFilterEpoch)
        assertEquals(SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE, signal.activity)
        assertEquals(20L.toBigInteger(), Int128Codec.toBigInt(signal.producerReportedAt.nanoseconds))
        assertEquals(10L.toBigInteger(), Int128Codec.toBigInt(signal.activitySince.nanoseconds))
    }

    @Test
    fun `a send that throws is reported and is not accepted`() {
        val refusal = OtlpTransportException("HTTP 409")
        val reports = heartbeat(send = { throw refusal }).report()
        assertEquals(listOf(CompletenessReport(sequence.epoch(), 0L, 0L, 0L, false)), reports)
        assertEquals(listOf<Throwable>(refusal), errors)
    }

    @Test
    fun `a Filter epoch regression sends nothing and reports the stable code`() {
        var filter = 3L
        val beat = heartbeat(filterEpoch = { filter })
        sequence.allocate()
        beat.report()
        filter = 2L
        assertEquals(emptyList(), beat.report())
        assertEquals(1, sent.size)
        assertEquals(SequenceException.FILTER_EPOCH_REGRESSED, (errors.single() as SequenceException).code)
    }

    @Test
    fun `a report requested during a send coalesces into one follow-up built after it`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val beat =
            heartbeat(send = { signal ->
                sent.add(signal)
                if (sent.size == 1) {
                    entered.countDown()
                    release.await()
                }
            })
        val first = thread { beat.report() }
        entered.await()
        val followers = mutableListOf<List<CompletenessReport>>()
        val waiting =
            List(3) {
                thread {
                    val reports = beat.report()
                    synchronized(followers) { followers.add(reports) }
                }
            }
        waiting.forEach { awaitParked(it) }
        sequence.allocate()
        release.countDown()
        first.join()
        waiting.forEach { it.join() }
        assertEquals(2, sent.size, "three requests during one send coalesce into one follow-up")
        assertEquals(0L, sent[0].lastEmitted)
        assertEquals(1L, sent[1].lastEmitted, "the follow-up is built after the blocked send")
        val expected = listOf(CompletenessReport(sequence.epoch(), 1L, 0L, 0L, true))
        assertEquals(List(3) { expected }, followers)
    }

    @Test
    fun `stop sends nothing and a late tick is inert`() {
        val scheduler = ManualScheduler()
        val beat = heartbeat(intervalMillis = 15_000L, scheduler = scheduler)
        beat.start()
        beat.stop()
        assertTrue(scheduler.tasks.single().cancelled)
        scheduler.tasks
            .single()
            .command
            .run()
        assertEquals(emptyList(), beat.report())
        assertEquals(emptyList(), beat.shutdown())
        assertEquals(emptyList(), sent)
    }

    @Test
    fun `shutdown waits for a running report and then sends once`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val beat =
            heartbeat(send = { signal ->
                sent.add(signal)
                if (sent.size == 1) {
                    entered.countDown()
                    release.await()
                }
            })
        val slot = sequence.allocate()
        val running = thread { beat.report() }
        entered.await()
        var final: List<CompletenessReport> = emptyList()
        val closing = thread { final = beat.shutdown() }
        awaitParked(closing)
        sequence.markExported(slot)
        release.countDown()
        running.join()
        closing.join()
        assertEquals(listOf(CompletenessReport(slot.epoch, 1L, 1L, 0L, true)), final)
        assertEquals(listOf(0L, 1L), sent.map { it.completeThrough })
        assertEquals(emptyList(), beat.report())
        assertEquals(2, sent.size)
    }

    @Test
    fun `the scheduler calls report once per tick and never after shutdown`() {
        val scheduler = ManualScheduler()
        val beat = heartbeat(intervalMillis = 15_000L, scheduler = scheduler)
        beat.start()
        beat.start()
        val task = scheduler.tasks.single()
        assertEquals(15_000L, task.initialDelayMillis)
        assertEquals(15_000L, task.delayMillis)
        task.command.run()
        task.command.run()
        assertEquals(2, sent.size)
        beat.shutdown()
        assertTrue(task.cancelled)
        assertEquals(3, sent.size)
        task.command.run()
        assertEquals(3, sent.size)
        assertFalse(scheduler.isShutdown, "an injected scheduler belongs to the host")
    }

    @Test
    fun `a failing scheduled report is reported and the schedule keeps running`() {
        val scheduler = ManualScheduler()
        val failure = IllegalStateException("clock unavailable")
        var failNext = false
        val beat =
            CompletenessHeartbeat(
                sequence = sequence,
                send = { sent.add(it) },
                nowUnixNanos = {
                    if (failNext) {
                        failNext = false
                        throw failure
                    }
                    clock.get()
                },
                intervalMillis = 15_000L,
                scheduler = scheduler,
                onError = { errors.add(it) },
            )
        failNext = true
        beat.start()
        scheduler.tasks
            .single()
            .command
            .run()
        assertEquals(listOf<Throwable>(failure), errors)
        scheduler.tasks
            .single()
            .command
            .run()
        assertEquals(1, sent.size)
    }

    @Test
    fun `an interval of zero schedules nothing`() {
        val scheduler = ManualScheduler()
        heartbeat(intervalMillis = 0L, scheduler = scheduler).start()
        assertEquals(emptyList(), scheduler.tasks)
    }

    @Test
    fun `the interval must be zero or between one second and the silence bound`() {
        for (interval in listOf(-1L, 999L, 60_000L)) {
            assertFailsWith<IllegalArgumentException>("interval $interval") { heartbeat(intervalMillis = interval) }
        }
        for (interval in listOf(0L, 1_000L, 59_999L)) heartbeat(intervalMillis = interval)
        assertEquals(15_000L, CompletenessHeartbeat.DEFAULT_INTERVAL_MILLIS)
    }

    // awaitParked waits until the thread blocks on the heartbeat's report lock.
    private fun awaitParked(waiter: Thread) {
        while (waiter.state != Thread.State.WAITING && waiter.isAlive) Thread.onSpinWait()
    }

    // ManualScheduler records scheduled work; a test runs a tick by calling the command.
    private class ManualScheduler :
        AbstractExecutorService(),
        ScheduledExecutorService {
        val tasks: MutableList<ManualTask> = ArrayList()
        private var shut = false

        override fun scheduleWithFixedDelay(
            command: Runnable,
            initialDelay: Long,
            delay: Long,
            unit: TimeUnit,
        ): ScheduledFuture<*> = ManualTask(command, unit.toMillis(initialDelay), unit.toMillis(delay)).also { tasks.add(it) }

        override fun scheduleAtFixedRate(
            command: Runnable,
            initialDelay: Long,
            period: Long,
            unit: TimeUnit,
        ): ScheduledFuture<*> = scheduleWithFixedDelay(command, initialDelay, period, unit)

        override fun schedule(
            command: Runnable,
            delay: Long,
            unit: TimeUnit,
        ): ScheduledFuture<*> = throw UnsupportedOperationException("one-shot scheduling is not used")

        override fun <V> schedule(
            callable: Callable<V>,
            delay: Long,
            unit: TimeUnit,
        ): ScheduledFuture<V> = throw UnsupportedOperationException("one-shot scheduling is not used")

        override fun execute(command: Runnable): Unit = throw UnsupportedOperationException("immediate execution is not used")

        override fun shutdown() {
            shut = true
        }

        override fun shutdownNow(): MutableList<Runnable> {
            shut = true
            return ArrayList()
        }

        override fun isShutdown(): Boolean = shut

        override fun isTerminated(): Boolean = shut

        override fun awaitTermination(
            timeout: Long,
            unit: TimeUnit,
        ): Boolean = shut
    }

    private class ManualTask(
        val command: Runnable,
        val initialDelayMillis: Long,
        val delayMillis: Long,
    ) : ScheduledFuture<Unit> {
        var cancelled = false

        override fun cancel(mayInterruptIfRunning: Boolean): Boolean {
            cancelled = true
            return true
        }

        override fun isCancelled(): Boolean = cancelled

        override fun isDone(): Boolean = cancelled

        override fun get() = Unit

        override fun get(
            timeout: Long,
            unit: TimeUnit,
        ) = Unit

        override fun getDelay(unit: TimeUnit): Long = unit.convert(delayMillis, TimeUnit.MILLISECONDS)

        override fun compareTo(other: Delayed): Int = getDelay(TimeUnit.MILLISECONDS).compareTo(other.getDelay(TimeUnit.MILLISECONDS))
    }
}
