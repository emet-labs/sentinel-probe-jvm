// OtlpLogReceiver serves POST /v1/logs. It binds 127.0.0.1 on an ephemeral port unless the host
// passes a bind host and port.
//
// A Probe in a container binds a non-loopback host ("0.0.0.0", or a specific interface) and a
// fixed port, so Sentinel can reach it over the container network. The receiver has no
// authentication: bind a non-loopback host only on a network that only Sentinel can reach.
//
// Verdict, EventFilter, and DecideResponse are the payloads Sentinel sends back.
// An unspecified decision action is an error. The gate reads the decoded
// DecideResponse from memory, waiting for it up to the request's remaining
// transport budget. This type does not call Connect.
package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.model.v1.Verdict
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import dev.emet.sentinel.probe.sdk.enforcement.DecideResult
import dev.emet.sentinel.probe.sdk.enforcement.Deps
import dev.emet.sentinel.probe.sdk.enforcement.GateOutcome
import dev.emet.sentinel.probe.sdk.enforcement.Options
import dev.emet.sentinel.probe.sdk.enforcement.gate
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.zip.GZIPInputStream
import java.util.zip.ZipException

private const val DEFAULT_HOST: String = "127.0.0.1"
private const val GZIP_LIMIT: Int = 8 * 1024 * 1024

// DecisionTimer arms the expiry of one decision wait. arm schedules onExpire after budgetNs
// nanoseconds and returns a handle whose close disarms it. A test supplies its own so a wait runs
// without a wall clock; it may call onExpire, or deliver a log, from inside arm.
public fun interface DecisionTimer {
    public fun arm(
        budgetNs: Long,
        onExpire: () -> Unit,
    ): AutoCloseable
}

public class OtlpLogReceiver
    @JvmOverloads
    public constructor(
        // host is the address to bind: the default loopback, or "0.0.0.0" / an interface address.
        bindHost: String = DEFAULT_HOST,
        // port is the TCP port to bind; 0 picks a free one.
        bindPort: Int = 0,
        // decisionTimer arms each decision wait. Null means a daemon scheduler the receiver owns
        // and shuts down in close().
        decisionTimer: DecisionTimer? = null,
    ) : AutoCloseable {
        private val server: HttpServer =
            HttpServer.create(InetSocketAddress(bindHost, bindPort), 0)
        private val verdicts = ArrayList<Verdict>()
        private val decisions = HashMap<String, DecideResponse>()
        private val decisionErrors = HashMap<String, Throwable>()
        private val waiters = HashMap<String, MutableList<CompletableFuture<DecideResponse>>>()
        private val filterListeners = ArrayList<(EventFilter) -> Unit>()
        private val lock = Any()
        private var closed = false

        // removeOnCancelPolicy drops a disarmed timer from the queue at once. Without it every
        // answered ask would keep its task, and the receiver and future it captures, queued until
        // the budget elapsed, which can be up to Long.MAX_VALUE ns.
        private val ownedScheduler: ScheduledExecutorService? =
            if (decisionTimer == null) {
                ScheduledThreadPoolExecutor(1) { task ->
                    Thread(task, "sentinel-decision-timer").apply { isDaemon = true }
                }.apply { removeOnCancelPolicy = true }
            } else {
                null
            }
        private val timer: DecisionTimer = decisionTimer ?: schedulerTimer(checkNotNull(ownedScheduler))

        // port is the TCP port actually bound.
        public val port: Int

        // logUrl is http://<host>:<port>/v1/logs, the shape SENTINEL_PROBE_LOG_URL requires. A
        // wildcard bind ("0.0.0.0") reports 127.0.0.1, the address that works on this machine; a
        // sender on another host uses this host's own name or address with [port].
        public val logUrl: String

        init {
            server.createContext(LOGS_PATH) { exchange -> handle(exchange) }
            server.start()
            port = server.address.port
            val shown = if (server.address.address.isAnyLocalAddress) DEFAULT_HOST else bindHost
            logUrl = "http://${if (shown.contains(':') && !shown.startsWith("[")) "[$shown]" else shown}:$port$LOGS_PATH"
        }

        public fun addFilterListener(listener: (EventFilter) -> Unit) {
            synchronized(lock) { filterListeners.add(listener) }
        }

        public fun verdicts(): List<Verdict> = synchronized(lock) { verdicts.toList() }

        // decide returns the DecideResponse decoded for this request id, waiting for it up to the
        // request's remaining transport budget (ADR-0024). It opens no socket and does not call
        // Connect.
        //
        //   - A refusal decoded for the id (an unspecified action) fails at once; a stored answer
        //     wins at once. Neither arms a wait.
        //   - An absent or zero budget arms no wait and returns a missing-decision error.
        //   - Otherwise the waiter is registered under the lock, one timer is armed outside it for
        //     exactly the budget (2^63 ns and above is capped at Long.MAX_VALUE), and decide blocks
        //     until a matching DecideResponse log, a refusal for the id, expiry, or close.
        //
        // Every error is an unresolved ask, and the gate applies its fail mode. A late answer does
        // not change an ask that has already expired.
        public fun decide(request: DecideRequest): DecideResult {
            val requestId = request.requestId
            val waiter = CompletableFuture<DecideResponse>()
            synchronized(lock) {
                decisionErrors[requestId]?.let { return DecideResult.Err(it) }
                decisions[requestId]?.let { return DecideResult.Ok(it) }
                if (closed) return DecideResult.Err(IllegalStateException("log receiver closed"))
                if (!request.hasRemainingTransportBudgetNanoseconds() || request.remainingTransportBudgetNanoseconds == 0L) {
                    return DecideResult.Err(IllegalStateException("no DecideResponse log for $requestId"))
                }
                waiters.getOrPut(requestId) { ArrayList() }.add(waiter)
            }
            val budgetNs = request.remainingTransportBudgetNanoseconds.let { if (it < 0L) Long.MAX_VALUE else it }
            val armed =
                try {
                    timer.arm(budgetNs) {
                        removeWaiter(requestId, waiter)
                        waiter.completeExceptionally(TimeoutException("timeout"))
                    }
                } catch (error: RejectedExecutionException) {
                    // The owned scheduler is shut down, so close() has normally failed this waiter
                    // already; this completes it if close() has not got there yet.
                    waiter.completeExceptionally(error)
                    AutoCloseable {}
                }
            try {
                return DecideResult.Ok(waiter.get())
            } catch (error: ExecutionException) {
                return DecideResult.Err(error.cause ?: error)
            } catch (error: InterruptedException) {
                Thread.currentThread().interrupt()
                removeWaiter(requestId, waiter)
                return DecideResult.Err(error)
            } finally {
                armed.close()
            }
        }

        // enforce runs the production gate against a decision this receiver decoded.
        // Budget and fail mode stay the gate's. An unspecified action is an error outcome.
        public fun enforce(
            event: ProducerEvent,
            filter: EventFilter?,
            deadlineNs: Long?,
            nowMonotonicNs: () -> Long,
            acceptedFailModeFor: ((SpecificationFilter) -> FailMode)?,
            options: Options,
        ): GateOutcome =
            gate(
                event,
                filter,
                deadlineNs,
                Deps(
                    decide = ::decide,
                    nowMonotonicNs = nowMonotonicNs,
                    acceptedFailModeFor = acceptedFailModeFor,
                ),
                options,
            )

        // close stops the server, fails every pending decide, and shuts down the owned scheduler.
        override fun close() {
            val pending: List<CompletableFuture<DecideResponse>>
            synchronized(lock) {
                closed = true
                pending = waiters.values.flatten()
                waiters.clear()
            }
            val error = IllegalStateException("log receiver closed")
            for (waiter in pending) waiter.completeExceptionally(error)
            server.stop(0)
            ownedScheduler?.shutdownNow()
        }

        private fun removeWaiter(
            requestId: String,
            waiter: CompletableFuture<DecideResponse>,
        ) {
            synchronized(lock) {
                val pending = waiters[requestId] ?: return
                pending.remove(waiter)
                if (pending.isEmpty()) waiters.remove(requestId)
            }
        }

        private fun handle(exchange: HttpExchange) {
            try {
                if (!exchange.requestMethod.equals("POST", ignoreCase = true)) {
                    respond(exchange, 405, "POST only".toByteArray())
                    return
                }
                val mediaType =
                    exchange.requestHeaders
                        .getFirst("Content-Type")
                        ?.substringBefore(';')
                        ?.trim()
                        ?.lowercase()
                if (mediaType != PROTOBUF_MEDIA_TYPE) {
                    respond(exchange, 415, "OTLP/HTTP requires application/x-protobuf".toByteArray())
                    return
                }
                val encoding =
                    exchange.requestHeaders
                        .getFirst("Content-Encoding")
                        ?.trim()
                        ?.lowercase()
                val raw = exchange.requestBody.readBytes()
                val bytes =
                    when (encoding) {
                        null, "", "identity" -> raw
                        "gzip" -> inflate(raw)
                        else -> {
                            respond(exchange, 415, "OTLP/HTTP accepts Content-Encoding identity or gzip".toByteArray())
                            return
                        }
                    }
                val inbound = OtlpCodec.decodeInbound(bytes)
                val listeners: List<(EventFilter) -> Unit>
                val filters = ArrayList<EventFilter>()
                val answered = ArrayList<Pair<CompletableFuture<DecideResponse>, DecideResponse>>()
                synchronized(lock) {
                    for (item in inbound) {
                        when (item) {
                            is InboundLog.VerdictLog -> verdicts.add(item.verdict)
                            is InboundLog.FilterLog -> filters.add(item.filter)
                            is InboundLog.DecisionLog -> answered.addAll(storeDecision(item.response))
                        }
                    }
                    listeners = filterListeners.toList()
                }
                for ((waiter, response) in answered) waiter.complete(response)
                // Each listener applies a pushed filter to its store, which refuses an older or
                // epoch-less one, so a reordered push never downgrades the Probe's filter.
                for (filter in filters) {
                    for (listener in listeners) listener(filter)
                }
                respond(exchange, 200, ByteArray(0), PROTOBUF_MEDIA_TYPE)
            } catch (error: UnspecifiedDecisionAction) {
                if (error.requestId.isNotEmpty()) {
                    val failed =
                        synchronized(lock) {
                            decisionErrors[error.requestId] = error
                            waiters.remove(error.requestId).orEmpty()
                        }
                    for (waiter in failed) waiter.completeExceptionally(error)
                }
                respond(exchange, 400, (error.message ?: "unspecified action").toByteArray())
            } catch (error: LogDecodeException) {
                respond(exchange, 400, (error.message ?: "log decode failed").toByteArray())
            } catch (error: SpanExportRefusal) {
                respond(exchange, 400, (error.message ?: "log decode failed").toByteArray())
            }
        }

        // storeDecision records an answer under the lock and returns the waiters it releases. A
        // later valid answer clears an earlier refusal for the id.
        private fun storeDecision(response: DecideResponse): List<Pair<CompletableFuture<DecideResponse>, DecideResponse>> {
            val requestId = response.requestId
            decisionErrors.remove(requestId)
            decisions[requestId] = response
            return waiters.remove(requestId).orEmpty().map { it to response }
        }

        private fun respond(
            exchange: HttpExchange,
            status: Int,
            body: ByteArray,
            contentType: String = "text/plain",
        ) {
            exchange.responseHeaders.set("Content-Type", contentType)
            exchange.sendResponseHeaders(status, body.size.toLong())
            exchange.responseBody.use { it.write(body) }
        }
    }

private fun inflate(bytes: ByteArray): ByteArray =
    try {
        GZIPInputStream(ByteArrayInputStream(bytes)).use { input -> readLimited(input) }
    } catch (error: ZipException) {
        throw LogDecodeException("gzip inflate failed: ${error.message}", error)
    }

private fun readLimited(input: GZIPInputStream): ByteArray {
    val output = ByteArrayOutputStream()
    val buffer = ByteArray(8 * 1024)
    var total = 0
    while (true) {
        val read = input.read(buffer)
        if (read < 0) return output.toByteArray()
        total += read
        if (total > GZIP_LIMIT) throw LogDecodeException("gzip inflate exceeded the uncompressed limit")
        output.write(buffer, 0, read)
    }
}

private fun schedulerTimer(scheduler: ScheduledExecutorService): DecisionTimer =
    DecisionTimer { budgetNs, onExpire ->
        val scheduled = scheduler.schedule(Runnable { onExpire() }, budgetNs, TimeUnit.NANOSECONDS)
        AutoCloseable { scheduled.cancel(false) }
    }
