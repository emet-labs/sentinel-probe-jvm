// OtlpExportingSpanProcessor posts every ended span to Sentinel through OtlpHttpExporter. The
// host attaches it to the TracerProvider. The processor does not call Connect.
//
// The span-end path never throws. onEnd runs inside Span.end(), in host code, so every failure
// in it (a refusal, an encode error, a full queue, a transport error, a throwing Filter supplier)
// is caught, marks the span's Sequence slot FAILED, and goes to ExportOptions.onError and a
// rate-limited log line. complete_through therefore never advances past a span Sentinel does not
// hold, and a broken export never breaks the host request.
//
// By default the export is asynchronous: onEnd only enqueues, and one daemon worker posts.
//   - The queue is bounded (queueCapacity). OVERFLOW POLICY: when it is full the span that just
//     ended is dropped, not an older queued span. A dropped Event cannot be re-sent, so its slot
//     stays FAILED for the life of the epoch; the epoch never closes over missing evidence.
//   - A transport error (no response, 408, 425, 429, 5xx) is retried with exponential backoff, at
//     most maxAttempts posts in all. A refusal or any other 4xx is not retried, because repeating
//     it changes nothing. A 2xx that reports rejected spans is not retried either.
//   - A span whose attempts ran out stays FAILED and is parked (at most parkedRetryCapacity, the
//     oldest forgotten first). While the queue is idle the worker re-posts the identical Event
//     every parkedRetryIntervalMillis; the allocator accepts FAILED -> EXPORTED, so
//     complete_through catches up once Sentinel is reachable again.
//   - forceFlush waits for every span queued before it. shutdown stops accepting spans, drains the
//     queue (without backoff sleeps), makes one last pass over the parked spans, and waits at most
//     shutdownTimeoutMillis. Shut the TracerProvider down BEFORE CompletenessHeartbeat.shutdown(),
//     so the closing heartbeat describes the flushed state.
// ExportOptions.synchronous posts inside onEnd instead, once and without retries; it still never
// throws.
//
// With a SequenceAllocator the processor stamps sentinel.sequence.epoch and
// sentinel.sequence.number when a span starts, before any Event Filter, unless the host already
// set both (a slot reserved ahead of its Event). It marks the slot exported only on HTTP 2xx with
// zero rejected spans.
//
// Event Filter: with ExportOptions.filter returning a held filter, a span no Specification
// selects is not exported and its slot is marked filtered under the filter's epoch. An empty
// store exports everything. Only whole spans are dropped; attributes are not trimmed. Until
// #542 a filtered slot does not advance complete_through.
package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SEQUENCE_NUMBER
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.Slot
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import dev.emet.sentinel.probe.sdk.filter.ApplyFilter
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanId
import io.opentelemetry.context.Context
import io.opentelemetry.sdk.common.CompletableResultCode
import io.opentelemetry.sdk.trace.ReadWriteSpan
import io.opentelemetry.sdk.trace.ReadableSpan
import io.opentelemetry.sdk.trace.SpanProcessor
import io.opentelemetry.sdk.trace.data.SpanData
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

// ExportOptions tunes OtlpExportingSpanProcessor. The defaults are the production policy.
public data class ExportOptions(
    // allowUnattributedRemoteParent exports a span whose REMOTE parent carries no
    // sentinel.parent.event.id (a browser, curl, a harness or any caller that is not a Probe) as
    // a causal ROOT: the Sentinel copy has no parent span id and no causal edge, so a Binding
    // that needs the edge stays Inconclusive. The span handed to the OTel pipeline is untouched.
    // Default false: such a span is refused and its slot FAILED. A LOCAL parent without the id
    // is refused either way.
    val allowUnattributedRemoteParent: Boolean = false,
    // filter reads the Event Filter held now, for example { client.currentFilter() }. Null (the
    // default) applies none.
    val filter: (() -> EventFilter?)? = null,
    // synchronous posts in onEnd, once, with no queue and no retry. Meant for tests and for
    // hosts that run their own queue.
    val synchronous: Boolean = false,
    // queueCapacity bounds the spans waiting for the worker. A span ending while it is full is
    // dropped and its slot marked FAILED.
    val queueCapacity: Int = DEFAULT_QUEUE_CAPACITY,
    // maxAttempts is the most posts one span gets on transport errors, the first included.
    val maxAttempts: Int = DEFAULT_MAX_ATTEMPTS,
    val initialBackoffMillis: Long = DEFAULT_INITIAL_BACKOFF_MILLIS,
    val maxBackoffMillis: Long = DEFAULT_MAX_BACKOFF_MILLIS,
    // parkedRetryCapacity bounds the FAILED spans kept for later re-posting.
    val parkedRetryCapacity: Int = DEFAULT_PARKED_CAPACITY,
    // parkedRetryIntervalMillis is how long the queue stays idle before the parked spans are
    // re-posted.
    val parkedRetryIntervalMillis: Long = DEFAULT_PARKED_INTERVAL_MILLIS,
    // shutdownTimeoutMillis bounds how long shutdown() waits for the drain.
    val shutdownTimeoutMillis: Long = DEFAULT_SHUTDOWN_TIMEOUT_MILLIS,
    // logIntervalMillis is the minimum gap between two log lines. 0 logs nothing.
    val logIntervalMillis: Long = DEFAULT_LOG_INTERVAL_MILLIS,
    // onError receives every export problem (not rate limited). It must not block, and a throw
    // from it is swallowed.
    val onError: (Throwable) -> Unit = {},
) {
    init {
        require(queueCapacity >= 1) { "queueCapacity must be at least 1" }
        require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
        require(initialBackoffMillis >= 0 && maxBackoffMillis >= initialBackoffMillis) {
            "backoff must satisfy 0 <= initialBackoffMillis <= maxBackoffMillis"
        }
        require(parkedRetryCapacity >= 0) { "parkedRetryCapacity must not be negative" }
        require(parkedRetryIntervalMillis >= 1) { "parkedRetryIntervalMillis must be at least 1" }
        require(shutdownTimeoutMillis >= 0) { "shutdownTimeoutMillis must not be negative" }
        require(logIntervalMillis >= 0) { "logIntervalMillis must not be negative" }
    }

    public companion object {
        public const val DEFAULT_QUEUE_CAPACITY: Int = 8192
        public const val DEFAULT_MAX_ATTEMPTS: Int = 6
        public const val DEFAULT_INITIAL_BACKOFF_MILLIS: Long = 250L
        public const val DEFAULT_MAX_BACKOFF_MILLIS: Long = 4_000L
        public const val DEFAULT_PARKED_CAPACITY: Int = 1024
        public const val DEFAULT_PARKED_INTERVAL_MILLIS: Long = 5_000L
        public const val DEFAULT_SHUTDOWN_TIMEOUT_MILLIS: Long = 5_000L
        public const val DEFAULT_LOG_INTERVAL_MILLIS: Long = 10_000L
    }
}

// SpanExportOverflow is a span dropped because the export queue was full.
public class SpanExportOverflow(
    message: String,
) : RuntimeException(message)

@Suppress("TooManyFunctions", "TooGenericExceptionCaught")
public class OtlpExportingSpanProcessor
    @JvmOverloads
    public constructor(
        private val exporter: OtlpHttpExporter,
        private val sequence: SequenceAllocator? = null,
        private val options: ExportOptions = ExportOptions(),
    ) : SpanProcessor {
        private class Job(
            val span: SpanData,
            val slot: Slot?,
        ) {
            var payload: ByteArray? = null
        }

        private sealed interface Task {
            class Export(
                val job: Job,
            ) : Task

            class Flush(
                val done: CompletableResultCode,
            ) : Task

            object Stop : Task
        }

        private enum class Outcome { DELIVERED, PARKABLE, PERMANENT }

        private val queue = LinkedBlockingQueue<Task>()
        private val pending = AtomicInteger()
        private val parked = ArrayDeque<Job>()
        private val closed = AtomicBoolean()

        @Volatile private var closing = false
        private val lastLogNanos = AtomicLong()
        private val logger: System.Logger = System.getLogger("dev.emet.sentinel.probe.sdk.export")

        private val worker: Thread? =
            if (options.synchronous) {
                null
            } else {
                Thread(::runWorker, "sentinel-span-export").apply {
                    isDaemon = true
                    start()
                }
            }

        override fun onStart(
            context: Context,
            parentSpan: ReadWriteSpan,
        ) {
            // Without an allocator, emission is an end-of-span concern.
            val allocator = sequence ?: return
            try {
                if (parentSpan.getAttribute(SEQUENCE_EPOCH_KEY) != null && parentSpan.getAttribute(SEQUENCE_NUMBER_KEY) != null) return
                val slot = allocator.allocate()
                parentSpan.setAttribute(SEQUENCE_EPOCH_KEY, slot.epoch)
                parentSpan.setAttribute(SEQUENCE_NUMBER_KEY, slot.number)
            } catch (error: Throwable) {
                // The span goes without a slot; its export is refused at the end. Never break the host.
                report(error)
            }
        }

        override fun isStartRequired(): Boolean = sequence != null

        override fun onEnd(span: ReadableSpan) {
            var slot: Slot? = null
            try {
                slot = slotOf(span)
                val job = prepare(span, slot) ?: return
                submit(job)
            } catch (error: Throwable) {
                failSlot(slot)
                report(error)
            }
        }

        override fun isEndRequired(): Boolean = true

        override fun forceFlush(): CompletableResultCode {
            if (worker == null) return CompletableResultCode.ofSuccess()
            val done = CompletableResultCode()
            if (closed.get() || !worker.isAlive) return done.also { it.fail() }
            queue.add(Task.Flush(done))
            return done
        }

        // shutdown stops accepting spans, flushes, and returns once the worker finished or
        // shutdownTimeoutMillis passed. Spans still unsent then are marked FAILED.
        override fun shutdown(): CompletableResultCode {
            if (!closed.compareAndSet(false, true)) return CompletableResultCode.ofSuccess()
            val thread = worker ?: return CompletableResultCode.ofSuccess()
            closing = true
            queue.add(Task.Stop)
            thread.join(options.shutdownTimeoutMillis)
            val clean = !thread.isAlive
            if (!clean) {
                thread.interrupt()
                thread.join(INTERRUPT_GRACE_MILLIS)
            }
            return if (failUndelivered() == 0 && clean) CompletableResultCode.ofSuccess() else CompletableResultCode.ofFailure()
        }

        // prepare validates what the host controls and applies the Event Filter. It returns null
        // when the span is filtered out. It throws for a span that cannot be exported.
        private fun prepare(
            span: ReadableSpan,
            slot: Slot?,
        ): Job? {
            var data = span.toSpanData()
            if (options.allowUnattributedRemoteParent && isUnattributedRemoteChild(data)) data = CausalRootSpanData(data)
            val filter = options.filter?.invoke()
            if (filter != null) {
                val event = OtlpCodec.eventOf(data)
                if (ApplyFilter.apply(event, filter) == null) {
                    slot?.let { sequence?.markFiltered(it, if (filter.hasEpoch()) filter.epoch else 0L) }
                    return null
                }
            }
            return Job(data, slot)
        }

        private fun isUnattributedRemoteChild(data: SpanData): Boolean {
            val parent = data.parentSpanContext
            return parent.isValid && parent.isRemote && data.attributes.get(PARENT_EVENT_ID_KEY).isNullOrBlank()
        }

        private fun submit(job: Job) {
            if (worker == null) {
                deliver(job, 1, quiet = false)
                return
            }
            if (closed.get()) throw SpanExportOverflow("span export is shut down; the span was not exported")
            if (pending.incrementAndGet() > options.queueCapacity) {
                pending.decrementAndGet()
                throw SpanExportOverflow(
                    "span export queue is full (${options.queueCapacity}); the span was dropped and its slot stays failed",
                )
            }
            queue.add(Task.Export(job))
        }

        private fun runWorker() {
            try {
                while (true) {
                    val task = queue.poll(options.parkedRetryIntervalMillis, TimeUnit.MILLISECONDS)
                    if (!handle(task)) return
                }
            } catch (ignored: InterruptedException) {
                Thread.currentThread().interrupt()
            } catch (error: Throwable) {
                report(error)
            }
        }

        // handle runs one task, null being an idle tick. It returns false when the worker must stop.
        private fun handle(task: Task?): Boolean {
            when (task) {
                null -> retryParked()
                is Task.Export -> exportTask(task)
                is Task.Flush -> task.done.succeed()
                Task.Stop -> {
                    retryParked()
                    return false
                }
            }
            return true
        }

        private fun exportTask(task: Task.Export) {
            try {
                deliver(task.job, options.maxAttempts, quiet = false)
            } finally {
                pending.decrementAndGet()
            }
        }

        // deliver posts the job, retrying transport errors up to attempts posts in all.
        private fun deliver(
            job: Job,
            attempts: Int,
            quiet: Boolean,
        ): Outcome {
            val payload =
                try {
                    job.payload ?: OtlpCodec.encodeTrace(listOf(job.span)).also { job.payload = it }
                } catch (error: Exception) {
                    return permanent(job, error)
                }
            var wait = options.initialBackoffMillis
            for (attempt in 1..attempts) {
                val outcome = post(job, payload, quiet)
                if (outcome != null) return outcome
                if (attempt == attempts || closing || !sleep(wait)) break
                wait = minOf(wait * 2, options.maxBackoffMillis)
            }
            failSlot(job.slot)
            if (!quiet && worker != null) park(job)
            return Outcome.PARKABLE
        }

        // post makes one attempt. It returns null for a retryable transport error.
        private fun post(
            job: Job,
            payload: ByteArray,
            quiet: Boolean,
        ): Outcome? =
            try {
                val rejected = exporter.postEncodedTrace(payload)
                if (rejected == 0L) {
                    job.slot?.let { sequence?.markExported(it) }
                    Outcome.DELIVERED
                } else {
                    permanent(job, SpanExportRefusal("Sentinel rejected $rejected span(s) of the export"))
                }
            } catch (error: OtlpTransportException) {
                if (!quiet) report(error)
                if (error.isRetryable) null else permanent(job, null)
            } catch (error: Exception) {
                permanent(job, error)
            }

        private fun permanent(
            job: Job,
            error: Throwable?,
        ): Outcome {
            failSlot(job.slot)
            if (error != null) report(error)
            return Outcome.PERMANENT
        }

        private fun sleep(millis: Long): Boolean =
            try {
                Thread.sleep(millis)
                true
            } catch (ignored: InterruptedException) {
                Thread.currentThread().interrupt()
                false
            }

        private fun park(job: Job) {
            if (options.parkedRetryCapacity == 0) return
            synchronized(parked) {
                if (parked.size >= options.parkedRetryCapacity) {
                    parked.removeFirst()
                    report(SpanExportOverflow("retry list is full (${options.parkedRetryCapacity}); the oldest failed span is not retried"))
                }
                parked.addLast(job)
            }
        }

        // retryParked re-posts the FAILED spans once each, oldest first, and stops at the first
        // transport error (Sentinel is still unreachable).
        private fun retryParked() {
            val batch = synchronized(parked) { parked.toList().also { parked.clear() } }
            for ((index, job) in batch.withIndex()) {
                if (Thread.currentThread().isInterrupted) {
                    requeue(batch.subList(index, batch.size))
                    return
                }
                if (deliver(job, 1, quiet = true) == Outcome.PARKABLE) {
                    requeue(batch.subList(index, batch.size))
                    return
                }
            }
        }

        private fun requeue(jobs: List<Job>) {
            synchronized(parked) { parked.addAll(0, jobs) }
        }

        // failUndelivered marks every span still queued FAILED and returns how many there were.
        private fun failUndelivered(): Int {
            var count = 0
            while (true) {
                val task = queue.poll() ?: return count
                if (task is Task.Export) {
                    failSlot(task.job.slot)
                    count += 1
                } else if (task is Task.Flush) {
                    task.done.fail()
                }
            }
        }

        private fun failSlot(slot: Slot?) {
            if (slot == null) return
            try {
                sequence?.markFailed(slot)
            } catch (error: Throwable) {
                // The allocator's own failure must not reach the host either.
                swallow(error)
            }
        }

        private fun slotOf(span: ReadableSpan): Slot? {
            val epoch = span.getAttribute(SEQUENCE_EPOCH_KEY)
            val number = span.getAttribute(SEQUENCE_NUMBER_KEY)
            return if (epoch != null && number != null) Slot(epoch, number) else null
        }

        // report hands the error to onError and, at most once per logIntervalMillis, the log.
        private fun report(error: Throwable) {
            try {
                options.onError(error)
            } catch (callbackError: Throwable) {
                swallow(callbackError)
            }
            val interval = options.logIntervalMillis
            if (interval == 0L) return
            val now = System.nanoTime()
            val last = lastLogNanos.get()
            if ((last == 0L || now - last > interval * NANOS_PER_MILLI) && lastLogNanos.compareAndSet(last, now)) {
                logger.log(System.Logger.Level.WARNING, "sentinel span export: ${error.javaClass.simpleName}: ${error.message}")
            }
        }

        private fun swallow(error: Throwable) {
            logger.log(System.Logger.Level.DEBUG, "sentinel span export: ignored ${error.javaClass.simpleName}", error)
        }

        private companion object {
            const val INTERRUPT_GRACE_MILLIS = 1_000L
            const val NANOS_PER_MILLI = 1_000_000L
            val SEQUENCE_EPOCH_KEY: AttributeKey<Long> = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_EPOCH)
            val SEQUENCE_NUMBER_KEY: AttributeKey<Long> = AttributeKey.longKey(ATTRIBUTE_SEQUENCE_NUMBER)
            val PARENT_EVENT_ID_KEY: AttributeKey<String> = AttributeKey.stringKey(ATTRIBUTE_PARENT_EVENT_ID)
        }
    }

// CausalRootSpanData is the span as Sentinel sees it when the remote caller's Event id is unknown:
// no parent span id at all. The span handed to the OTel pipeline is untouched.
private class CausalRootSpanData(
    private val data: SpanData,
) : SpanData by data {
    override fun getParentSpanContext(): SpanContext = SpanContext.getInvalid()

    @Suppress("OVERRIDE_DEPRECATION")
    override fun getParentSpanId(): String = SpanId.getInvalid()
}
