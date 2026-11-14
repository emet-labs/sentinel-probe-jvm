// OtlpLogReceiver serves POST /v1/logs. It binds 127.0.0.1 on an ephemeral port unless the host
// passes a bind host and port.
//
// A Probe in a container binds a non-loopback host ("0.0.0.0", or a specific interface) and a
// fixed port, so Sentinel can reach it over the container network. The receiver has no
// authentication: bind a non-loopback host only on a network that only Sentinel can reach.
//
// Verdict, EventFilter, and DecideResponse are the payloads Sentinel sends back.
// An unspecified decision action is an error. The gate reads the decoded
// DecideResponse from memory. This type does not call Connect.
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
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.net.InetSocketAddress
import java.util.zip.GZIPInputStream
import java.util.zip.ZipException

private const val DEFAULT_HOST: String = "127.0.0.1"
private const val GZIP_LIMIT: Int = 8 * 1024 * 1024

public class OtlpLogReceiver
    @JvmOverloads
    public constructor(
        // host is the address to bind: the default loopback, or "0.0.0.0" / an interface address.
        bindHost: String = DEFAULT_HOST,
        // port is the TCP port to bind; 0 picks a free one.
        bindPort: Int = 0,
    ) : AutoCloseable {
        private val server: HttpServer =
            HttpServer.create(InetSocketAddress(bindHost, bindPort), 0)
        private val verdicts = ArrayList<Verdict>()
        private val decisions = HashMap<String, dev.emet.sentinel.probe.v1.DecideResponse>()
        private val decisionErrors = HashMap<String, Throwable>()
        private val filterListeners = ArrayList<(EventFilter) -> Unit>()
        private val lock = Any()

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

        // decide returns the DecideResponse already decoded for this request id.
        // It opens no socket and does not call Connect.
        public fun decide(request: DecideRequest): DecideResult {
            synchronized(lock) {
                decisionErrors[request.requestId]?.let { return DecideResult.Err(it) }
                val response = decisions[request.requestId]
                return if (response == null) {
                    DecideResult.Err(IllegalStateException("no DecideResponse log for ${request.requestId}"))
                } else {
                    DecideResult.Ok(response)
                }
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

        override fun close() {
            server.stop(0)
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
                synchronized(lock) {
                    for (item in inbound) {
                        when (item) {
                            is InboundLog.VerdictLog -> verdicts.add(item.verdict)
                            is InboundLog.FilterLog -> filters.add(item.filter)
                            is InboundLog.DecisionLog -> decisions[item.response.requestId] = item.response
                        }
                    }
                    listeners = filterListeners.toList()
                }
                for (filter in filters) {
                    for (listener in listeners) listener(filter)
                }
                respond(exchange, 200, ByteArray(0), PROTOBUF_MEDIA_TYPE)
            } catch (error: UnspecifiedDecisionAction) {
                if (error.requestId.isNotEmpty()) {
                    synchronized(lock) { decisionErrors[error.requestId] = error }
                }
                respond(exchange, 400, (error.message ?: "unspecified action").toByteArray())
            } catch (error: LogDecodeException) {
                respond(exchange, 400, (error.message ?: "log decode failed").toByteArray())
            } catch (error: SpanExportRefusal) {
                respond(exchange, 400, (error.message ?: "log decode failed").toByteArray())
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
