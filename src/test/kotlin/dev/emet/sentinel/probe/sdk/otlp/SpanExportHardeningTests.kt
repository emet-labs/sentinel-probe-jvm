package dev.emet.sentinel.probe.sdk.otlp

import com.sun.net.httpserver.HttpServer
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.Sensitivity
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_CLAIMED_SENSITIVITY
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_PARENT_EVENT_ID
import dev.emet.sentinel.probe.sdk.emission.ATTRIBUTE_SCHEMA_VERSION
import dev.emet.sentinel.probe.sdk.emission.FilteredRange
import dev.emet.sentinel.probe.sdk.emission.ProbeSpanProcessor
import dev.emet.sentinel.probe.sdk.emission.SequenceAllocator
import dev.emet.sentinel.probe.sdk.emission.SpanExportRefusal
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
import io.opentelemetry.sdk.trace.SdkTracerProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.Collections
import java.util.HexFormat
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SpanExportHardeningTests {
    private val clock = AtomicLong(1_794_000_000_000_000_000L)
    private val sequence = SequenceAllocator(nowUnixNanos = clock::get)
    private val errors: MutableList<Throwable> = Collections.synchronizedList(ArrayList())
    private val cleanup = ArrayList<() -> Unit>()

    @AfterEach
    fun tearDown() {
        cleanup.asReversed().forEach { it() }
    }

    // --- never throw from the span-end path ---

    @Test
    fun `an unreachable Sentinel does not throw out of end and marks the slot failed`() {
        val processor = processor("http://127.0.0.1:1", ExportOptions(synchronous = true, onError = errors::add))
        val span =
            provider(processor)
                .get("t")
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
        span.end()
        assertEquals(0L, sequence.declaration().completeThrough)
        assertFailedSlot(1L)
        assertTrue(errors.single() is OtlpTransportException)
    }

    @Test
    fun `a throwing onError callback and a throwing Filter supplier do not reach the host`() {
        val processor =
            processor(
                "http://127.0.0.1:1",
                ExportOptions(synchronous = true, filter = { error("filter store broke") }, onError = { error("callback broke") }),
            )
        val span =
            provider(processor)
                .get("t")
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
        span.end()
        assertFailedSlot(1L)
    }

    @Test
    fun `a throwing ProbeSpanProcessor callback does not throw out of end`() {
        val seen = ArrayList<Throwable>()
        val processor = ProbeSpanProcessor(onEnd = { error("convert broke") }, onError = seen::add)
        provider(processor)
            .get("t")
            .spanBuilder("x")
            .startSpan()
            .end()
        assertEquals("convert broke", seen.single().message)
    }

    // --- allowUnattributedRemoteParent ---

    @Test
    fun `a foreign remote parent is refused by default and the slot is failed without throwing`() {
        withSentinel { sentinel ->
            val processor = processor(sentinel.origin, ExportOptions(synchronous = true, onError = errors::add))
            remoteChild(provider(processor)).end()
            assertEquals(0, sentinel.requests.get())
            assertTrue(errors.single() is SpanExportRefusal)
            assertFailedSlot(1L)
        }
    }

    @Test
    fun `with the option a foreign remote parent is exported as a causal root with no parent span id`() {
        withSentinel { sentinel ->
            val processor =
                processor(sentinel.origin, ExportOptions(synchronous = true, allowUnattributedRemoteParent = true, onError = errors::add))
            remoteChild(provider(processor)).end()
            assertTrue(errors.isEmpty(), "unexpected $errors")
            val wire = sentinel.spans().single()
            assertTrue(wire.parentSpanId.isEmpty, "the exported copy names no parent span id")
            assertFalse(wire.attributesList.any { it.key == ATTRIBUTE_PARENT_EVENT_ID })
            assertEquals(1L, sequence.declaration().completeThrough)
        }
    }

    @Test
    fun `a remote parent that carries an Event id keeps its edge under the option`() {
        withSentinel { sentinel ->
            val processor = processor(sentinel.origin, ExportOptions(synchronous = true, allowUnattributedRemoteParent = true))
            remoteChild(provider(processor), parentEventId = "parent-event").end()
            val wire = sentinel.spans().single()
            assertFalse(wire.parentSpanId.isEmpty)
            assertEquals(
                "parent-event",
                wire.attributesList
                    .single { it.key == ATTRIBUTE_PARENT_EVENT_ID }
                    .value.stringValue,
            )
        }
    }

    @Test
    fun `a local parent without an Event id is still refused under the option`() {
        withSentinel { sentinel ->
            val processor =
                processor(sentinel.origin, ExportOptions(synchronous = true, allowUnattributedRemoteParent = true, onError = errors::add))
            val tracer = provider(processor).get("t")
            val parent = tracer.spanBuilder("parent").also(::contract).startSpan()
            val child =
                tracer
                    .spanBuilder("sagashop.order.child")
                    .setParent(Context.root().with(parent))
                    .also(::contract)
                    .startSpan()
            child.end()
            assertTrue(errors.single() is SpanExportRefusal)
            assertEquals(0, sentinel.requests.get())
        }
    }

    // --- async exporter ---

    @Test
    fun `the default exporter is asynchronous and exports every span`() {
        withSentinel { sentinel ->
            val processor = processor(sentinel.origin, ExportOptions(onError = errors::add))
            val tracer = provider(processor).get("t")
            repeat(5) {
                tracer
                    .spanBuilder("sagashop.order.placed")
                    .also(::contract)
                    .startSpan()
                    .end()
            }
            assertTrue(processor.forceFlush().join(5, TimeUnit.SECONDS).isSuccess)
            assertEquals(5, sentinel.spans().size)
            assertEquals(5L, sequence.declaration().completeThrough)
        }
    }

    @Test
    fun `a transport error is retried with backoff and then marks the slot exported`() {
        withSentinel(respond = { n -> if (n <= 2) 503 else 200 }) { sentinel ->
            val processor = processor(sentinel.origin, fast())
            tracerOf(processor)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            assertTrue(processor.forceFlush().join(5, TimeUnit.SECONDS).isSuccess)
            assertEquals(3, sentinel.requests.get())
            assertEquals(1L, sequence.declaration().completeThrough)
        }
    }

    @Test
    fun `a 400 is not retried and the slot stays failed`() {
        withSentinel(respond = { 400 }) { sentinel ->
            val processor = processor(sentinel.origin, fast(parkedRetryCapacity = 0))
            tracerOf(processor)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            processor.forceFlush().join(5, TimeUnit.SECONDS)
            assertEquals(1, sentinel.requests.get())
            assertFailedSlot(1L)
        }
    }

    @Test
    fun `a span whose attempts ran out is parked and re-posted until Sentinel accepts it`() {
        val up =
            java.util.concurrent.atomic
                .AtomicBoolean(false)
        withSentinel(respond = { if (up.get()) 200 else 503 }) { sentinel ->
            val processor = processor(sentinel.origin, fast(maxAttempts = 2))
            tracerOf(processor)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            awaitTrue { sentinel.requests.get() >= 2 }
            assertEquals(0L, sequence.declaration().completeThrough, "failed, never declared complete")
            up.set(true)
            awaitTrue { sequence.declaration().completeThrough == 1L }
        }
    }

    @Test
    fun `a full queue drops the span that just ended and marks its slot failed`() {
        val release = CountDownLatch(1)
        withSentinel(respond = {
            release.await(10, TimeUnit.SECONDS)
            200
        }) { sentinel ->
            val processor = processor(sentinel.origin, fast(queueCapacity = 1, parkedRetryCapacity = 0))
            val tracer = provider(processor).get("t")
            repeat(3) {
                tracer
                    .spanBuilder("sagashop.order.placed")
                    .also(::contract)
                    .startSpan()
                    .end()
            }
            assertTrue(errors.any { it is SpanExportOverflow }, "an overflow was reported")
            release.countDown()
            processor.forceFlush().join(5, TimeUnit.SECONDS)
            val declaration = sequence.declaration()
            assertEquals(3L, declaration.lastEmitted)
            assertTrue(declaration.completeThrough < 3L, "complete_through never passes a dropped slot")
        }
    }

    @Test
    fun `shutdown flushes queued spans before returning`() {
        withSentinel(respond = {
            Thread.sleep(20)
            200
        }) { sentinel ->
            val processor = processor(sentinel.origin, fast())
            val tracer = tracerOf(processor)
            repeat(4) {
                tracer
                    .spanBuilder("sagashop.order.placed")
                    .also(::contract)
                    .startSpan()
                    .end()
            }
            assertTrue(processor.shutdown().isSuccess)
            assertEquals(4, sentinel.spans().size)
            assertEquals(4L, sequence.declaration().completeThrough)
        }
    }

    // --- Event Filter on the span path ---

    @Test
    fun `a held Filter that selects nothing drops the span and marks its slot filtered`() {
        withSentinel { sentinel ->
            val processor = processor(sentinel.origin, ExportOptions(synchronous = true, filter = { filterFor("other.kind") }))
            tracerOf(processor)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            assertEquals(0, sentinel.requests.get())
            assertEquals(listOf(FilteredRange(1L, 1L, 7L)), sequence.declaration().filtered)
        }
    }

    @Test
    fun `a held Filter that selects the span exports it, and no Filter exports everything`() {
        withSentinel { sentinel ->
            val selecting = processor(sentinel.origin, ExportOptions(synchronous = true, filter = { filterFor("sagashop.order.placed") }))
            tracerOf(selecting)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            val empty = processor(sentinel.origin, ExportOptions(synchronous = true, filter = { null }))
            tracerOf(empty)
                .spanBuilder("sagashop.order.placed")
                .also(::contract)
                .startSpan()
                .end()
            assertEquals(2, sentinel.spans().size)
            assertEquals(emptyList(), sequence.declaration().filtered)
        }
    }

    // --- log receiver bind address ---

    @Test
    fun `the log receiver binds loopback by default and a configured host and port on request`() {
        OtlpLogReceiver().use { receiver ->
            assertTrue(receiver.logUrl.startsWith("http://127.0.0.1:${receiver.port}/"))
        }
        OtlpLogReceiver("0.0.0.0", 0).use { receiver ->
            assertTrue(receiver.port > 0)
            assertEquals("http://127.0.0.1:${receiver.port}/v1/logs", receiver.logUrl)
            val connection =
                java.net.URI
                    .create(receiver.logUrl)
                    .toURL()
                    .openConnection() as java.net.HttpURLConnection
            assertEquals(405, connection.responseCode, "reachable, and POST only")
        }
    }

    // --- helpers ---

    private fun fast(
        maxAttempts: Int = 4,
        queueCapacity: Int = 64,
        parkedRetryCapacity: Int = 16,
    ) = ExportOptions(
        queueCapacity = queueCapacity,
        maxAttempts = maxAttempts,
        initialBackoffMillis = 1,
        maxBackoffMillis = 4,
        parkedRetryCapacity = parkedRetryCapacity,
        parkedRetryIntervalMillis = 25,
        logIntervalMillis = 0,
        onError = errors::add,
    )

    private fun filterFor(kind: String): EventFilter =
        EventFilter
            .newBuilder()
            .setEpoch(7L)
            .addSpecifications(
                SpecificationFilter
                    .newBuilder()
                    .setSpecificationId("spec")
                    .setSpecificationVersion("1")
                    .setEventMatch(EventMatch.newBuilder().addEventKinds(kind).build())
                    .build(),
            ).build()

    private fun processor(
        origin: String,
        options: ExportOptions,
    ): OtlpExportingSpanProcessor =
        OtlpExportingSpanProcessor(OtlpHttpExporter(origin, "sagashop-order"), sequence, options).also { cleanup.add { it.shutdown() } }

    private val providers = HashMap<Any, SdkTracerProvider>()

    private fun provider(processor: io.opentelemetry.sdk.trace.SpanProcessor): SdkTracerProvider =
        providers.getOrPut(processor) { SdkTracerProvider.builder().addSpanProcessor(processor).build() }

    private fun tracerOf(processor: io.opentelemetry.sdk.trace.SpanProcessor) = provider(processor).get("t")

    private fun contract(builder: io.opentelemetry.api.trace.SpanBuilder) {
        builder
            .setAttribute(ATTRIBUTE_EVENT_ID, "evt-${java.util.UUID.randomUUID()}")
            .setAttribute(ATTRIBUTE_SCHEMA_VERSION, "sagashop-contract-v1")
            .setAttribute(ATTRIBUTE_ACKNOWLEDGED_FILTER_EPOCH, 0L)
            .setAttribute(ATTRIBUTE_CLAIMED_SENSITIVITY, Sensitivity.SENSITIVITY_INTERNAL.name)
    }

    // remoteChild is a span whose parent came from a caller outside this process (a traceparent).
    private fun remoteChild(
        provider: SdkTracerProvider,
        parentEventId: String? = null,
    ): Span {
        val remote =
            SpanContext.createFromRemoteParent(
                "0af7651916cd43dd8448eb211c80319c",
                "b7ad6b7169203331",
                TraceFlags.getSampled(),
                TraceState.getDefault(),
            )
        val builder =
            provider
                .get("t")
                .spanBuilder("sagashop.order.placed")
                .setParent(Context.root().with(Span.wrap(remote)))
        contract(builder)
        if (parentEventId != null) builder.setAttribute(ATTRIBUTE_PARENT_EVENT_ID, parentEventId)
        return builder.startSpan()
    }

    // assertFailedSlot: the slot is FAILED rather than in flight, so markFiltered is a no-op.
    private fun assertFailedSlot(number: Long) {
        sequence.markFiltered(
            dev.emet.sentinel.probe.sdk.emission
                .Slot(sequence.epoch(), number),
            1L,
        )
        assertEquals(emptyList(), sequence.declaration().filtered)
        assertEquals(0L, sequence.declaration().completeThrough)
    }

    private fun awaitTrue(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "condition not reached" }
            Thread.sleep(5)
        }
    }

    private class FakeSentinel(
        val origin: String,
    ) {
        val requests = AtomicInteger()
        val bodies: MutableList<ByteArray> = Collections.synchronizedList(ArrayList())

        fun spans() =
            bodies.flatMap { body ->
                ExportTraceServiceRequest.parseFrom(body).resourceSpansList.flatMap { rs ->
                    rs.scopeSpansList.flatMap { it.spansList }
                }
            }
    }

    private fun withSentinel(
        respond: (Int) -> Int = { 200 },
        block: (FakeSentinel) -> Unit,
    ) {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.executor =
            java.util.concurrent.Executors
                .newCachedThreadPool()
        val sentinel = FakeSentinel("http://127.0.0.1:${server.address.port}")
        server.createContext("/") { exchange ->
            val body = exchange.requestBody.readBytes()
            val n = sentinel.requests.incrementAndGet()
            val status = respond(n)
            if (status in 200..299) sentinel.bodies.add(body)
            val reply = if (status in 200..299) ByteArray(0) else "status $status".toByteArray()
            exchange.sendResponseHeaders(status, if (reply.isEmpty()) -1 else reply.size.toLong())
            exchange.responseBody.use { it.write(reply) }
        }
        server.start()
        try {
            block(sentinel)
        } finally {
            providers.values.forEach { it.shutdown().join(5, TimeUnit.SECONDS) }
            server.stop(0)
        }
    }
}
