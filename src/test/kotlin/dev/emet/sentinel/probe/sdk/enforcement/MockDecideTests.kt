package dev.emet.sentinel.probe.sdk.enforcement

import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.SpecificationDecision
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertNull

// MockDecider is the in-process Decide stub, mirroring tests/enforcement/mock-decide-server.ts
// and Go's mockdecide_test.go: a canned response, a scripted sequence whose last element
// repeats, or a forced error, while recording every request and the call count.
//
// Deliberately not an HTTP server: the gate calls the dependency function directly, and
// ConnectLoopbackTests / TransportTests cover the real wire separately.
class MockDecider(
    var response: DecideResponse? = null,
    private val sequence: List<DecideResponse> = emptyList(),
    private val error: Throwable? = null,
) {
    private val requests = mutableListOf<DecideRequest>()
    private val nextIndex = AtomicInteger(0)
    private val lastReq = AtomicReference<DecideRequest?>(null)

    fun decide(request: DecideRequest): DecideResult {
        synchronized(this) { requests.add(request); lastReq.set(request) }
        error?.let { return DecideResult.Err(it) }
        if (sequence.isNotEmpty()) {
            val idx = nextIndex.getAndIncrement().coerceAtMost(sequence.lastIndex)
            return DecideResult.Ok(sequence[idx])
        }
        if (response != null) return DecideResult.Ok(response)
        return DecideResult.Ok(
            DecideResponse.newBuilder()
                .setRequestId(request.requestId)
                .setAction(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_PERMIT)
                .build(),
        )
    }

    fun callCount(): Int = synchronized(this) { requests.size }

    fun lastRequest(): DecideRequest? = lastReq.get()

    fun requests(): List<DecideRequest> = synchronized(this) { requests.toList() }
}

class MockDecideTests {
    private fun makeResponse(
        action: dev.emet.sentinel.probe.v1.DecisionAction,
        vararg decisions: SpecificationDecision,
    ): DecideResponse =
        DecideResponse.newBuilder()
            .setRequestId("mock")
            .setAction(action)
            .addAllSpecifications(decisions.toList())
            .build()

    @Test
    fun `canned response repeats`() {
        val mock = MockDecider(response = makeResponse(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY))
        val req = DecideRequest.newBuilder().setRequestId("r1").build()
        val r1 = mock.decide(req)
        val r2 = mock.decide(req)
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY, (r1 as DecideResult.Ok).response!!.action)
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY, (r2 as DecideResult.Ok).response!!.action)
        assertEquals(2, mock.callCount())
    }

    @Test
    fun `scripted sequence repeats the last element`() {
        val mock = MockDecider(
            sequence = listOf(
                makeResponse(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_PERMIT),
                makeResponse(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY),
            ),
        )
        val req = DecideRequest.newBuilder().setRequestId("r").build()
        val a = (mock.decide(req) as DecideResult.Ok).response!!.action
        val b = (mock.decide(req) as DecideResult.Ok).response!!.action
        val c = (mock.decide(req) as DecideResult.Ok).response!!.action
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_PERMIT, a)
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY, b)
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY, c, "last element repeats")
    }

    @Test
    fun `forced error is surfaced`() {
        val mock = MockDecider(error = RuntimeException("connection refused"))
        val res = mock.decide(DecideRequest.newBuilder().setRequestId("r").build())
        assertTrue(res is DecideResult.Err)
        assertEquals("connection refused", res.error.message)
    }

    @Test
    fun `default response is a permit echoing the request id`() {
        val mock = MockDecider()
        val res = mock.decide(DecideRequest.newBuilder().setRequestId("echo").build()) as DecideResult.Ok
        assertEquals(dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_PERMIT, res.response!!.action)
        assertEquals("echo", res.response!!.requestId)
    }

    @Test
    fun `records the last request and count`() {
        val mock = MockDecider()
        assertNull(mock.lastRequest())
        mock.decide(DecideRequest.newBuilder().setRequestId("first").build())
        mock.decide(DecideRequest.newBuilder().setRequestId("second").build())
        assertEquals(2, mock.callCount())
        assertEquals("second", mock.lastRequest()!!.requestId)
    }
}
