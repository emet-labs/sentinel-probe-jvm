// CompletenessHeartbeat reports a SequenceAllocator's declaration to Sentinel (#461).
//
// Each report() builds one ProducerCompletenessSignal from the allocator's declare() at the
// moment it is sent: last_emitted, complete_through and acknowledged_filter_epoch always set
// with presence, the producer clock domain "unix" with zero uncertainty. Between two reports
// with nothing new, every field except producer_reported_at's nanoseconds is identical, so
// Sentinel sees a restatement and writes no Verdict revision.
//
// Guarantees, in order of importance:
//   - complete_through comes from the allocator, so it never runs ahead of a successful export;
//   - a report never regresses the epoch, the counters or the acknowledged Filter epoch, and
//     never advances a declared close (the allocator opens a new epoch instead);
//   - before the first accepted signal of a new epoch, the previous epoch's final declaration is
//     restated once per report, so a close whose post failed, or a Filter acknowledgement that
//     arrived while the epoch was closed, is not lost at the roll.
//
// Reports run one at a time. A report() requested while one is sending coalesces with every
// other such request into one follow-up, built after the running one. The clock, the send
// function and the ScheduledExecutorService are injected, so tests read no wall clock and arm
// no timer.
package dev.emet.sentinel.probe.sdk.otlp

import dev.emet.sentinel.model.v1.OccurrenceTime
import dev.emet.sentinel.model.v1.ProducerCompletenessSignal
import dev.emet.sentinel.model.v1.SourceActivityState
import dev.emet.sentinel.probe.sdk.emission.EpochDeclaration
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.SequenceException
import dev.emet.sentinel.probe.sdk.emission.systemUnixNanos
import dev.emet.sentinel.probe.sdk.int128.Int128Codec
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

// CompletenessReport describes one signal a report sent, and whether Sentinel accepted it.
public data class CompletenessReport(
    val sequenceEpoch: Long,
    val lastEmitted: Long,
    val completeThrough: Long,
    val acknowledgedFilterEpoch: Long,
    val accepted: Boolean,
)

// CompletenessSignals builds the wire signal the heartbeat sends.
public object CompletenessSignals {
    public const val CLOCK_DOMAIN: String = "unix"

    public fun build(
        declaration: EpochDeclaration,
        acknowledgedFilterEpoch: Long,
        activity: SourceActivityState,
        reportedAtUnixNanos: Long,
        activitySinceUnixNanos: Long,
    ): ProducerCompletenessSignal =
        ProducerCompletenessSignal
            .newBuilder()
            .setSequenceEpoch(declaration.epoch)
            .setLastEmitted(declaration.lastEmitted)
            .setCompleteThrough(declaration.completeThrough)
            .setAcknowledgedFilterEpoch(acknowledgedFilterEpoch)
            .setProducerReportedAt(unixTime(reportedAtUnixNanos))
            .setActivity(activity)
            .setActivitySince(unixTime(activitySinceUnixNanos))
            .build()

    private fun unixTime(nanos: Long): OccurrenceTime =
        OccurrenceTime
            .newBuilder()
            .setClockDomainId(CLOCK_DOMAIN)
            .setNanoseconds(Int128Codec.fromInt64(nanos))
            .setUncertaintyNanoseconds(0)
            .build()
}

@Suppress("TooManyFunctions")
public class CompletenessHeartbeat(
    // sequence is the allocator whose declaration this heartbeat reports.
    private val sequence: SequenceAllocator,
    // send posts one signal and returns normally only when Sentinel accepted it, for example
    // OtlpHttpExporter::exportCompleteness. A throw is a signal that was not accepted.
    private val send: (ProducerCompletenessSignal) -> Unit,
    // filterEpoch reads the acknowledged Filter epoch, for example FilterStore::epoch. Null is
    // no Filter held, sent as 0.
    private val filterEpoch: () -> Long? = { null },
    // nowUnixNanos is the clock.
    private val nowUnixNanos: () -> Long = ::systemUnixNanos,
    // intervalMillis is the report cadence: 0 (no timer, report() only) or 1000 to 59999.
    private val intervalMillis: Long = DEFAULT_INTERVAL_MILLIS,
    // scheduler runs the periodic report. Null starts a daemon thread owned by this heartbeat.
    // An injected scheduler belongs to the host and is never shut down here.
    private val scheduler: ScheduledExecutorService? = null,
    // onError receives every signal Sentinel did not accept, a suppressed report
    // (SequenceException FILTER_EPOCH_REGRESSED) and any failure of a scheduled report.
    private val onError: (Throwable) -> Unit = {},
) {
    private data class Accepted(
        val declaration: EpochDeclaration,
        val acknowledgedFilterEpoch: Long,
    )

    private data class Run(
        val id: Long,
        val reports: List<CompletenessReport>,
    )

    private data class Moment(
        val reportedAt: Long,
        val activity: SourceActivityState,
        val activitySince: Long,
    )

    // runLock serialises reports; lastAccepted and ackFloor are touched only under it.
    private val runLock = ReentrantLock()
    private val runsStarted = AtomicLong()

    @Volatile private var lastRun: Run? = null
    private var lastAccepted: Accepted? = null
    private val ackFloor = HashMap<Long, Long>()

    // stateLock guards the activity fields, which declareIdle and declareActive change from any
    // thread.
    private val stateLock = Any()
    private var activity = SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE
    private var activitySince: Long = nowUnixNanos()
    private var idleMark: Pair<Long, Long>? = null
    private var lastReportedAt = 0L

    @Volatile private var stopped = false
    private val scheduleLock = Any()
    private var scheduled: ScheduledFuture<*>? = null
    private var ownedScheduler: ScheduledExecutorService? = null

    init {
        require(intervalMillis == 0L || intervalMillis in MIN_INTERVAL_MILLIS..MAX_INTERVAL_MILLIS) {
            "heartbeat interval must be 0 or $MIN_INTERVAL_MILLIS..$MAX_INTERVAL_MILLIS ms, got $intervalMillis"
        }
    }

    // start schedules report() every interval. An interval of 0 schedules nothing.
    public fun start() {
        if (intervalMillis == 0L) return
        synchronized(scheduleLock) {
            if (stopped || scheduled != null) return
            val executor = scheduler ?: newDaemonScheduler().also { ownedScheduler = it }
            scheduled = executor.scheduleWithFixedDelay(::scheduledReport, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS)
        }
    }

    // report sends the current declaration and returns one CompletenessReport per signal sent.
    // A call made while another report runs waits for, and returns, the one follow-up report
    // built after it.
    public fun report(): List<CompletenessReport> {
        val requestedAfter = runsStarted.get()
        runLock.withLock {
            lastRun?.let { if (it.id > requestedAfter) return it.reports }
            val id = runsStarted.incrementAndGet()
            val reports = runOnce()
            lastRun = Run(id, reports)
            return reports
        }
    }

    // declareIdle declares the source INTENTIONALLY_IDLE. The next allocation ends it.
    public fun declareIdle() {
        synchronized(stateLock) {
            if (activity == SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE) return
            activity = SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE
            activitySince = maxOf(nowUnixNanos(), lastReportedAt)
            val declaration = sequence.declaration()
            idleMark = declaration.epoch to declaration.lastEmitted
        }
    }

    // declareActive declares the source ACTIVE again.
    public fun declareActive() {
        synchronized(stateLock) {
            if (activity == SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE) return
            activity = SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE
            activitySince = maxOf(nowUnixNanos(), lastReportedAt)
        }
    }

    // stop cancels the schedule and silences the heartbeat. It sends nothing, for a process
    // that must go quiet.
    public fun stop() {
        stopped = true
        cancelSchedule()
    }

    // shutdown cancels the schedule, waits for a running report, sends one final report and
    // then silences the heartbeat. The final signal is a close only if nothing is unresolved.
    public fun shutdown(): List<CompletenessReport> {
        cancelSchedule()
        runLock.withLock {
            val reports = runOnce()
            stopped = true
            return reports
        }
    }

    private fun runOnce(): List<CompletenessReport> {
        if (stopped) return emptyList()
        val declaration = sequence.declare()
        val ack = filterEpoch() ?: 0L
        val moment = advanceClock(declaration.epoch, declaration.lastEmitted)
        val reports = ArrayList<CompletenessReport>()
        val previous = declaration.previous
        if (previous != null &&
            lastAccepted?.declaration?.epoch != declaration.epoch &&
            lastAccepted != Accepted(previous, ack) &&
            ack >= ackFloor.getOrDefault(previous.epoch, 0L)
        ) {
            sendOne(previous, ack, moment)?.let { reports.add(it) }
        }
        val floor = ackFloor.getOrDefault(declaration.epoch, 0L)
        if (ack < floor) {
            // Never clamp upward: that would acknowledge a Filter the Probe is not applying.
            val message = "acknowledged Filter epoch $ack is below $floor already sent for epoch ${declaration.epoch}"
            onError(SequenceException(SequenceException.FILTER_EPOCH_REGRESSED, message))
            return reports
        }
        val current = EpochDeclaration(declaration.epoch, declaration.lastEmitted, declaration.completeThrough)
        sendOne(current, ack, moment)?.let { reports.add(it) }
        val oldest = previous?.epoch ?: declaration.epoch
        ackFloor.keys.removeIf { it < oldest }
        return reports
    }

    // advanceClock fixes the report instant, never before an earlier report or the activity
    // change, and ends an idle declaration if the source allocated since.
    private fun advanceClock(
        epoch: Long,
        lastEmitted: Long,
    ): Moment =
        synchronized(stateLock) {
            val now = maxOf(nowUnixNanos(), lastReportedAt, activitySince)
            lastReportedAt = now
            if (activity == SourceActivityState.SOURCE_ACTIVITY_STATE_INTENTIONALLY_IDLE && idleMark != (epoch to lastEmitted)) {
                activity = SourceActivityState.SOURCE_ACTIVITY_STATE_ACTIVE
                activitySince = now
            }
            Moment(now, activity, activitySince)
        }

    // sendOne posts one signal. Null means the heartbeat was stopped and nothing was sent.
    @Suppress("TooGenericExceptionCaught")
    private fun sendOne(
        declaration: EpochDeclaration,
        ack: Long,
        moment: Moment,
    ): CompletenessReport? {
        if (stopped) return null
        val signal = CompletenessSignals.build(declaration, ack, moment.activity, moment.reportedAt, moment.activitySince)
        ackFloor[declaration.epoch] = maxOf(ackFloor.getOrDefault(declaration.epoch, 0L), ack)
        val accepted =
            try {
                send(signal)
                true
            } catch (error: RuntimeException) {
                // Any refusal from the host's send function (HTTP, transport, a 409) is a signal
                // Sentinel did not accept. It changes nothing but the report.
                onError(error)
                false
            }
        if (accepted) lastAccepted = Accepted(declaration, ack)
        return CompletenessReport(declaration.epoch, declaration.lastEmitted, declaration.completeThrough, ack, accepted)
    }

    @Suppress("TooGenericExceptionCaught")
    private fun scheduledReport() {
        try {
            report()
        } catch (error: RuntimeException) {
            // A scheduled executor stops a task that throws; report and keep the cadence.
            onError(error)
        }
    }

    private fun cancelSchedule() {
        synchronized(scheduleLock) {
            scheduled?.cancel(false)
            scheduled = null
            ownedScheduler?.shutdown()
            ownedScheduler = null
        }
    }

    public companion object {
        public const val DEFAULT_INTERVAL_MILLIS: Long = 15_000L
        public const val MIN_INTERVAL_MILLIS: Long = 1_000L

        // Below the 60 s active-silence bound.
        public const val MAX_INTERVAL_MILLIS: Long = 59_999L

        private fun newDaemonScheduler(): ScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor { runnable ->
                Thread(runnable, "sentinel-completeness-heartbeat").apply { isDaemon = true }
            }
    }
}
