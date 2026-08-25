package dev.emet.sentinel.probe.sdk.client

import com.google.protobuf.ByteString
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import org.junit.jupiter.api.Test
import java.net.Authenticator
import java.net.CookieHandler
import java.net.InetSocketAddress
import java.net.ProxySelector
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

// TransportTests — Connect framing golden fixtures. Tests the transport's encoding/decoding
// without a live server beyond loopback, using the JDK's built-in com.sun.net.httpserver to
// capture the wire request and return canned proto responses. Verifies that the transport sends
// Content-Type: application/proto and Connect-Protocol-Version: 1, encodes the request as raw
// protobuf, decodes a binary protobuf response, and parses Connect error envelopes (always
// JSON). This is the JVM analog of sdk/python/tests/test_transport.py.
class TransportTests {
    private class ThrowingHttpClient(
        private val throwable: Throwable,
    ) : HttpClient() {
        override fun cookieHandler(): Optional<CookieHandler> = Optional.empty()

        override fun connectTimeout(): Optional<Duration> = Optional.empty()

        override fun followRedirects(): Redirect = Redirect.NEVER

        override fun proxy(): Optional<ProxySelector> = Optional.empty()

        override fun sslContext(): SSLContext = SSLContext.getDefault()

        override fun sslParameters(): SSLParameters = SSLParameters()

        override fun authenticator(): Optional<Authenticator> = Optional.empty()

        override fun version(): Version = Version.HTTP_1_1

        override fun executor(): Optional<Executor> = Optional.empty()

        override fun <T> send(
            request: HttpRequest,
            responseBodyHandler: HttpResponse.BodyHandler<T>,
        ): HttpResponse<T> = throw throwable

        override fun <T> sendAsync(
            request: HttpRequest,
            responseBodyHandler: HttpResponse.BodyHandler<T>,
        ): CompletableFuture<HttpResponse<T>> = error("not used")

        override fun <T> sendAsync(
            request: HttpRequest,
            responseBodyHandler: HttpResponse.BodyHandler<T>,
            pushPromiseHandler: HttpResponse.PushPromiseHandler<T>,
        ): CompletableFuture<HttpResponse<T>> = error("not used")
    }

    private fun makeRequest(): DecideRequest =
        DecideRequest
            .newBuilder()
            .setRequestId("req-1")
            .setIdempotencyKey("idem-1")
            .setSourceHandle("gateway.tool-calls")
            .setProducerEvent(
                ProducerEvent
                    .newBuilder()
                    .setId("evt-1")
                    .setKind("transfer.initiated")
                    .addAttributes(
                        AttributeEntry
                            .newBuilder()
                            .setKey("sagashop.order.id")
                            .setValue(AttributeValue.newBuilder().setStringValue("ord-9").build())
                            .build(),
                    ).build(),
            ).build()

    private class CapturingContext {
        var contentType: String? = null
        var connectVersion: String? = null
        var body: ByteArray = ByteArray(0)
        var statusHandler: (HttpExchange) -> Unit = { it.sendResponseHeaders(200, -1) }
    }

    private fun httpServer(ctx: CapturingContext): HttpServer {
        val server = HttpServer.create(InetSocketAddress(0), 0)
        server.createContext("/") { exchange ->
            ctx.contentType = exchange.requestHeaders.getFirst("Content-Type")
            ctx.connectVersion = exchange.requestHeaders.getFirst("Connect-Protocol-Version")
            ctx.body = exchange.requestBody.readBytes()
            ctx.statusHandler(exchange)
        }
        server.start()
        return server
    }

    private fun sendProto(
        exchange: HttpExchange,
        response: DecideResponse,
    ) {
        val bytes = response.toByteArray()
        exchange.responseHeaders.set("Content-Type", "application/proto")
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.write(bytes)
        exchange.responseBody.close()
    }

    private fun sendError(
        exchange: HttpExchange,
        status: Int,
        body: String,
    ) {
        val bytes = body.toByteArray(Charsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.write(bytes)
        exchange.responseBody.close()
    }

    @Test
    fun `sends application-proto and connect-protocol-version 1`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        ctx.statusHandler = { sendProto(it, DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()) }
        try {
            val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}"))
            transport.decide(makeRequest())
        } finally {
            server.stop(0)
        }
        assertEquals("application/proto", ctx.contentType)
        assertEquals("1", ctx.connectVersion)
    }

    @Test
    fun `encodes the request as raw protobuf`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        ctx.statusHandler = { sendProto(it, DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()) }
        val request = makeRequest()
        try {
            SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}")).decide(request)
        } finally {
            server.stop(0)
        }
        // The wire body must equal the request's own binary protobuf encoding (a golden fixture:
        // the transport must not re-frame or wrap the message).
        val expected = request.toByteArray()
        assertTrue(ctx.body.contentEquals(expected), "wire body must equal request.toByteArray()")
        // And it must parse back to an equal message.
        val parsed = DecideRequest.parseFrom(ctx.body)
        assertEquals(request, parsed)
    }

    @Test
    fun `decodes a binary protobuf response`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        val canned =
            DecideResponse
                .newBuilder()
                .setRequestId("r")
                .setAction(DecisionAction.DECISION_ACTION_DENY)
                .build()
        ctx.statusHandler = { sendProto(it, canned) }
        try {
            val response = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}")).decide(makeRequest())
            assertEquals(DecisionAction.DECISION_ACTION_DENY, response.action)
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `parses a connect error envelope`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        ctx.statusHandler = { sendError(it, 503, """{"code":"unavailable","message":"sentinel down"}""") }
        try {
            val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}"))
            try {
                transport.decide(makeRequest())
                fail("expected ConnectError")
            } catch (e: ConnectError) {
                assertEquals("unavailable", e.code)
                assertEquals("sentinel down", e.errorMessage)
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `a non-json error body still raises ConnectError with the HTTP status`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        ctx.statusHandler = { sendError(it, 500, "<html>boom</html>") }
        try {
            try {
                SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}")).decide(makeRequest())
                fail("expected ConnectError")
            } catch (e: ConnectError) {
                assertEquals("unknown", e.code)
                assertTrue(e.errorMessage.contains("HTTP 500"), "errorMessage = ${e.errorMessage}")
            }
        } finally {
            server.stop(0)
        }
    }

    @Test
    fun `an unreachable endpoint raises a raw transport error`() {
        // Nothing is listening: the client's own transport error reaches the caller as-is.
        val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:1"))
        try {
            transport.decide(makeRequest())
            fail("expected an exception")
        } catch (e: Exception) {
            // Not a ConnectError — a raw JDK IOException/connection error.
            assertTrue(e !is ConnectError)
        }
    }

    @Test
    fun `decideFunc returns Ok on success and Err on failure`() {
        val ctx = CapturingContext()
        val server = httpServer(ctx)
        ctx.statusHandler = { sendProto(it, DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()) }
        try {
            val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:${server.address.port}"))
            val ok = decideFunc(transport)(makeRequest())
            assertTrue(ok is dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Ok)
        } finally {
            server.stop(0)
        }
        // Error path: unreachable endpoint yields Err.
        val transport = SentinelTransport(TransportOptions(baseUrl = "http://127.0.0.1:1"))
        val err = decideFunc(transport)(makeRequest())
        assertTrue(err is dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Err)
    }

    @Test
    fun `decideFunc preserves every transport throwable by identity`() {
        val sentinel = AssertionError("sentinel transport failure")
        val transport =
            SentinelTransport(
                TransportOptions(
                    baseUrl = "http://sentinel.invalid",
                    httpClient = ThrowingHttpClient(sentinel),
                ),
            )

        val result = decideFunc(transport)(makeRequest())

        assertTrue(result is dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Err)
        assertSame(sentinel, result.error)
    }
}
