// Transport — Connect-over-Java-HttpClient client for Sentinel's decision endpoint.
// Kotlin analog of sdk/go/client/transport.go and sdk/python/.../transport.py.
//
// Connect is the right transport for the same reasons the Go and TypeScript references chose
// it: it is built directly on HTTP/1.1 (the JDK's java.net.http.HttpClient is the stdlib
// equivalent), which is exactly the "stdlib host language" issue #33 names.
//
// There is no official Connect library for the JVM that targets Kotlin without extra ceremony,
// so — like the Python SDK — this is a hand-written transport: it issues a POST with
// Content-Type: application/proto and Connect-Protocol-Version: 1, encodes the request as raw
// protobuf bytes (Message.toByteArray) and decodes the response the same way
// (Message.parseFrom). Connect error envelopes are always JSON, parsed by the tiny
// JsonErrorEnvelope reader below (no JSON library pulled onto the runtime classpath).
package dev.emet.sentinel.probe.sdk.client

import com.google.protobuf.Message
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

internal const val CONNECT_PROTOCOL_VERSION = "1"
internal const val DECIDE_PATH = "/sentinel.probe.v1.SentinelDecisionService/Decide"

// ConnectError is a Connect-protocol error returned by the server. Carries the Connect code
// (e.g. "unavailable") and the server-side message, so the enforcement gate can classify it
// distinctly from a raw transport error (see enforcement.describeError).
public class ConnectError(
    override val code: String,
    public val errorMessage: String,
) : RuntimeException("connect-$code: $errorMessage"),
    dev.emet.sentinel.probe.sdk.enforcement.CodedError

// TransportOptions configures the Connect client for Sentinel's decision endpoint.
public data class TransportOptions(
    // baseURL of the Sentinel decision endpoint, e.g. "http://sentinel.local:7070".
    public val baseUrl: String,
    // httpClient to issue requests with. Defaults to a new HttpClient when null. Hosts override
    // it to install timeouts, connection pools, or an authenticator.
    public val httpClient: HttpClient? = null,
)

// SentinelTransport is a Connect client for SentinelDecisionService.
//
// decide performs the actual RPC over the JDK HttpClient with binary protobuf framing. It is
// the analog of the Go SDK's generated probev1connect.SentinelDecisionServiceClient —
// hand-written here because no Connect library targets the JVM at this layer.
public class SentinelTransport(private val options: TransportOptions) {
    private val baseUrl: String = options.baseUrl.trimEnd('/')
    private val client: HttpClient = options.httpClient ?: HttpClient.newHttpClient()

    // decide performs the Decide RPC. Encodes the request as binary protobuf, POSTs it with
    // application/proto content type, and decodes the response. A non-2xx response is parsed as
    // a Connect error envelope (always JSON) and raised as ConnectError.
    public fun decide(request: DecideRequest): DecideResponse {
        val wire = request.toByteArray()
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + DECIDE_PATH))
            .header("Content-Type", "application/proto")
            .header("Connect-Protocol-Version", CONNECT_PROTOCOL_VERSION)
            .POST(HttpRequest.BodyPublishers.ofByteArray(wire))
            .build()
        val response = client.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() != 200) raiseConnectError(response)
        return DecideResponse.parseFrom(response.body())
    }

    private fun raiseConnectError(response: HttpResponse<ByteArray>) {
        val body = response.body()
        val text = if (body != null) String(body, Charsets.UTF_8) else ""
        val parsed = tryParseConnectError(text)
        throw ConnectError(
            parsed?.first ?: "unknown",
            parsed?.second ?: "HTTP ${response.statusCode()}: ${text.take(200)}",
        )
    }
}

// tryParseConnectError parses a Connect error envelope (always JSON with `code` and `message`
// fields) without a JSON library. Returns null for a non-JSON body so the caller falls back to
// a raw HTTP status.
internal fun tryParseConnectError(text: String): Pair<String, String>? {
    val code = extractJsonString(text, "code") ?: return null
    val message = extractJsonString(text, "message") ?: ""
    return code to message
}

// extractJsonString finds the string value for a top-level JSON key via a minimal scan. It only
// understands flat object bodies with string values, which is the Connect error envelope shape;
// anything else returns null so the caller treats the body as opaque.
private fun extractJsonString(text: String, key: String): String? {
    val needle = "\"$key\""
    var i = text.indexOf(needle)
    while (i >= 0) {
        var j = i + needle.length
        while (j < text.length && text[j].isWhitespace()) j++
        if (j < text.length && text[j] == ':') {
            j++
            while (j < text.length && text[j].isWhitespace()) j++
            if (j < text.length && text[j] == '"') {
                return readJsonString(text, j)
            }
        }
        i = text.indexOf(needle, i + 1)
    }
    return null
}

private fun readJsonString(text: String, startQuote: Int): String {
    val sb = StringBuilder()
    var k = startQuote + 1
    while (k < text.length) {
        val c = text[k++]
        when {
            c == '"' -> return sb.toString()
            c == '\\' && k < text.length -> {
                val esc = text[k++]
                sb.append(when (esc) {
                    '"' -> '"'; '\\' -> '\\'; '/' -> '/'; 'b' -> '\b'
                    'f' -> '\u000C'; 'n' -> '\n'; 'r' -> '\r'; 't' -> '\t'
                    'u' -> {
                        val hex = text.substring(k, minOf(k + 4, text.length))
                        k += 4
                        hex.toIntOrNull(16)?.toChar() ?: return sb.toString()
                    }
                    else -> esc
                })
            }
            else -> sb.append(c)
        }
    }
    return sb.toString()
}

// decideFunc adapts a SentinelTransport to the enforcement gate's decide dependency shape
// (DecideResult). Driving the shipped adapter here rather than a local copy is the point: this
// exercises the production path end to end. Errors are returned unwrapped so the gate can
// classify ConnectError distinctly from a raw transport error.
public fun decideFunc(transport: SentinelTransport): (DecideRequest) -> dev.emet.sentinel.probe.sdk.enforcement.DecideResult =
    { request ->
        try {
            dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Ok(transport.decide(request))
        } catch (t: Throwable) {
            dev.emet.sentinel.probe.sdk.enforcement.DecideResult.Err(t)
        }
    }
