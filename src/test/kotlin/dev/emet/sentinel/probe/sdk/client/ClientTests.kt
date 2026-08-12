package dev.emet.sentinel.probe.sdk.client

import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecisionAction
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.sdk.enforcement.DecideResult
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ClientTests {
    private val testBaseUrl = "http://sentinel.local:7070"

    private fun newTestClient(config: ProbeClient.Config): ProbeClient =
        ProbeClient(config) { DecideResult.Ok(DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()) }

    private fun makeEvent(id: String, kind: String): ProducerEvent =
        ProducerEvent.newBuilder().setId(id).setKind(kind).setSchemaVersion("sentinel.model.v1").build()

    private fun filterWithEpoch(epoch: Long): EventFilter =
        EventFilter.newBuilder().setEpoch(epoch).build()

    @Test
    fun `has no filter before first refresh`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl))
        assertNull(client.currentFilter())
        assertNull(client.acknowledgedEpoch())
    }

    @Test
    fun `setFilter swaps and stamps epoch`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl))
        assertTrue(client.setFilter(filterWithEpoch(7L)))
        assertEquals(7L, client.acknowledgedEpoch())
        assertEquals(7L, client.currentFilter()!!.epoch)
    }

    @Test
    fun `setFilter same epoch is a no-op`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(7L)))
        assertFalse(client.setFilter(filterWithEpoch(7L)))
    }

    @Test
    fun `refreshOnEpoch reports whether a fetch is warranted`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(7L)))
        assertFalse(client.refreshOnEpoch(7L))
        assertTrue(client.refreshOnEpoch(8L))
        assertFalse(client.refreshOnEpoch(null), "no announced epoch means nothing to compare against")
    }

    @Test
    fun `buildDecideRequest stamps identifiers, event, epoch and budget`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "gateway.tool-calls", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(5L)))
        val request = client.buildDecideRequest(makeEvent("evt-1", "x"), "req-1", "idem-1", 4000L)
        assertEquals("req-1", request.requestId)
        assertEquals("idem-1", request.idempotencyKey)
        assertEquals("gateway.tool-calls", request.sourceHandle)
        assertEquals(5L, request.filterEpoch)
        assertEquals("evt-1", request.producerEvent.id)
        assertEquals(4000L, request.remainingTransportBudgetNanoseconds)
    }

    @Test
    fun `buildDecideRequest omits absent budget`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(5L)))
        val request = client.buildDecideRequest(makeEvent("evt-1", "x"), "req-1", "idem-1", null)
        assertFalse(request.hasRemainingTransportBudgetNanoseconds(), "an absent budget must leave the field absent")
    }

    @Test
    fun `buildDecideRequest omits epoch when no filter held`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl))
        val request = client.buildDecideRequest(makeEvent("evt-1", "x"), "req-1", "idem-1", null)
        assertFalse(request.hasFilterEpoch(), "no filter held means no epoch on the wire")
    }

    @Test
    fun `buildDecideRequest carries epoch zero`() {
        // The epoch-0 trap at the client: epoch 0 is present and stamped on the wire.
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(0L)))
        val request = client.buildDecideRequest(makeEvent("evt-1", "x"), "req-1", "idem-1", null)
        assertTrue(request.hasFilterEpoch())
        assertEquals(0L, request.filterEpoch)
    }

    @Test
    fun `initial filter seeds the store`() {
        val client = newTestClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl, initialFilter = filterWithEpoch(9L)))
        assertEquals(9L, client.acknowledgedEpoch())
        assertEquals(9L, client.currentFilter()!!.epoch)
    }

    @Test
    fun `exposes decider and sourceHandle`() {
        val decider: (DecideRequest) -> DecideResult = { DecideResult.Ok(DecideResponse.newBuilder().build()) }
        val client = ProbeClient(ProbeClient.Config(sourceHandle = "gateway.tool-calls", sentinelBaseUrl = testBaseUrl), decider)
        assertEquals("gateway.tool-calls", client.sourceHandle())
        assertEquals(decider, client.decider())
    }

    @Test
    fun `decideFunc exposes the decide dependency shape`() {
        val decider: (DecideRequest) -> DecideResult = { DecideResult.Ok(DecideResponse.newBuilder().setAction(DecisionAction.DECISION_ACTION_PERMIT).build()) }
        val client = ProbeClient(ProbeClient.Config(sourceHandle = "src", sentinelBaseUrl = testBaseUrl), decider)
        val result = client.decideFunc()(DecideRequest.newBuilder().setRequestId("r").build()) as DecideResult.Ok
        assertEquals(DecisionAction.DECISION_ACTION_PERMIT, result.response!!.action)
    }
}
