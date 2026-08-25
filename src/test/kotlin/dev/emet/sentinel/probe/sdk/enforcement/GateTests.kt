package dev.emet.sentinel.probe.sdk.enforcement

import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.EventMatch
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.sdk.client.ConnectError
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.DecisionAction
import dev.emet.sentinel.probe.v1.SpecificationDecision
import dev.emet.sentinel.probe.v1.UnresolvedReason
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.TimeoutException
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class GateTests {
    private val testKind = "transfer.initiated"
    private val testEpoch = 5L
    private val testOptions =
        Options(
            sourceHandle = "gateway.tool-calls",
            requestID = "req-1",
            idempotencyKey = "idem-1",
        )

    private fun u64(v: Long): Long = v

    private fun i64(v: Long): Long = v

    private fun makeEvent(kind: String): ProducerEvent =
        ProducerEvent
            .newBuilder()
            .setId("evt-1")
            .setKind(kind)
            .setSchemaVersion("sentinel.model.v1")
            .build()

    private fun makeSpec(
        specificationID: String,
        kinds: List<String>,
        failMode: FailMode,
        deliveryMode: DeliveryMode,
    ): SpecificationFilter =
        SpecificationFilter
            .newBuilder()
            .setSpecificationId(specificationID)
            .setSpecificationVersion("1.0.0")
            .setEventMatch(
                EventMatch
                    .newBuilder()
                    .addAllEventKinds(kinds)
                    .setDeliveryMode(deliveryMode)
                    .build(),
            ).setFailMode(failMode)
            .setEvaluationMode(dev.emet.sentinel.model.v1.EvaluationMode.EVALUATION_MODE_ENFORCE)
            .setReadiness(dev.emet.sentinel.model.v1.Readiness.READINESS_ACTIVE)
            .setLatencyBudgetNanoseconds(10_000L)
            .build()

    private fun askAndBlockSpec(): SpecificationFilter =
        makeSpec("spec-1", listOf(testKind), FailMode.FAIL_MODE_OPEN, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK)

    private fun closedAskAndBlockSpec(): SpecificationFilter =
        makeSpec("spec-1", listOf(testKind), FailMode.FAIL_MODE_CLOSED, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK)

    private fun shipAsyncSpec(): SpecificationFilter =
        makeSpec("spec-1", listOf(testKind), FailMode.FAIL_MODE_OPEN, DeliveryMode.DELIVERY_MODE_SHIP_ASYNC)

    private fun makeFilter(
        epoch: Long?,
        vararg specs: SpecificationFilter,
    ): EventFilter {
        val b = EventFilter.newBuilder().addAllSpecifications(specs.toList())
        if (epoch != null) b.setEpoch(epoch)
        return b.build()
    }

    private val alwaysClosed: (SpecificationFilter) -> FailMode = { FailMode.FAIL_MODE_CLOSED }
    private val alwaysOpen: (SpecificationFilter) -> FailMode = { FailMode.FAIL_MODE_OPEN }

    private fun makeDeps(
        mock: MockDecider,
        nowNs: Long,
        accepted: ((SpecificationFilter) -> FailMode)? = alwaysOpen,
    ): Deps =
        Deps(
            decide = mock::decide,
            nowMonotonicNs = { nowNs },
            acceptedFailModeFor = accepted,
        )

    private fun makeResponse(
        action: DecisionAction,
        vararg decisions: SpecificationDecision,
    ) = dev.emet.sentinel.probe.v1.DecideResponse
        .newBuilder()
        .setRequestId("mock")
        .setAction(action)
        .addAllSpecifications(decisions.toList())
        .build()

    private fun makeDecision(
        specID: String,
        action: DecisionAction,
        unresolved: UnresolvedReason? = null,
    ) = SpecificationDecision
        .newBuilder()
        .setSpecificationId(specID)
        .setSpecificationVersion("1.0.0")
        .setAction(action)
        .also { if (unresolved != null) it.setUnresolvedReason(unresolved) }
        .build()

    @Test
    fun `permit`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Permit)
        assertEquals(testEpoch, outcome.filterEpoch)
        assertTrue(outcome.permitted)
    }

    @Test
    fun `deny`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DENY))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Deny)
        assertFalse(outcome.permitted)
    }

    @Test
    fun `defer with budget remaining`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DEFER))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Defer)
        assertEquals(1, mock.callCount())
    }

    @Test
    fun `defer budget exhausted fails open`() {
        // The clock advances between entry and response, so the post-response budget check sees 0.
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DEFER))
        var calls = 0
        val deps =
            makeDeps(mock, 0).copy(nowMonotonicNs = {
                calls++
                if (calls == 1) 0L else 10000L // entry: budget remains; after response: budget gone
            })
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), deps, testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals("defer-budget-exhausted", outcome.reason)
        assertEquals(1, mock.callCount(), "the ask happened before the budget ran out")
    }

    @Test
    fun `defer budget exhausted fails closed when contracted`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DEFER))
        val deps =
            makeDeps(mock, 0, alwaysClosed).copy(nowMonotonicNs = {
                // First call returns 0 (entry), subsequent return 10000 (after response).
                if (mock.callCount() == 0) 0L else 10000L
            })
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000), deps, testOptions)
        assertTrue(outcome is GateOutcome.FailClosedDeny)
    }

    @Test
    fun `transport error fails open by default`() {
        val mock = MockDecider(error = RuntimeException("connection refused"))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals("transport-error: connection refused", outcome.reason)
    }

    @Test
    fun `transport error fails closed when contracted`() {
        val mock = MockDecider(error = RuntimeException("connection refused"))
        val outcome =
            gate(
                makeEvent(testKind),
                makeFilter(u64(testEpoch), closedAskAndBlockSpec()),
                i64(10000),
                makeDeps(mock, 0, alwaysClosed),
                testOptions,
            )
        assertTrue(outcome is GateOutcome.FailClosedDeny)
    }

    @Test
    fun `fail closed requires an accepted contract`() {
        // Declaring CLOSED is not enough. An operator has to have agreed to be blocked,
        // otherwise the mode downgrades to OPEN.
        val mock = MockDecider(error = RuntimeException("connection refused"))
        val outcome =
            gate(
                makeEvent(testKind),
                makeFilter(u64(testEpoch), closedAskAndBlockSpec()),
                i64(10000),
                makeDeps(mock, 0, alwaysOpen),
                testOptions,
            )
        assertTrue(outcome is GateOutcome.FailOpenPermit, "declared CLOSED, contract not accepted -> downgrade to fail-open")
    }

    @Test
    fun `nil acceptedFailModeFor throws`() {
        // A missing contract source is a wiring bug, not a default. Silently treating it as
        // "nothing accepted" would make it the only dependency whose absence WEAKENS enforcement.
        val mock = MockDecider(error = RuntimeException("boom"))
        val deps = Deps(decide = mock::decide, nowMonotonicNs = { 0L }, acceptedFailModeFor = null)
        val ex =
            assertThrows<IllegalStateException> {
                gate(makeEvent(testKind), makeFilter(u64(testEpoch), closedAskAndBlockSpec()), i64(10000), deps, testOptions)
            }
        assertTrue(ex.message!!.contains("Deps.acceptedFailModeFor is required"))
    }

    @Test
    fun `without enforcing specs no contract source is needed`() {
        val mock = MockDecider()
        val deps = Deps(decide = mock::decide, nowMonotonicNs = { 0L }, acceptedFailModeFor = null)
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), shipAsyncSpec()), i64(10000), deps, testOptions)
        assertTrue(outcome is GateOutcome.Permit)
    }

    @Test
    fun `aggregate fail closed wins`() {
        // One contracted-closed spec among many open ones decides the aggregate.
        val mock = MockDecider(error = RuntimeException("down"))
        val filter =
            makeFilter(
                u64(testEpoch),
                makeSpec("open-1", listOf(testKind), FailMode.FAIL_MODE_OPEN, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK),
                makeSpec("open-2", listOf(testKind), FailMode.FAIL_MODE_OPEN, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK),
                closedAskAndBlockSpec(),
            )
        val outcome = gate(makeEvent(testKind), filter, i64(10000), makeDeps(mock, 0, alwaysClosed), testOptions)
        assertTrue(outcome is GateOutcome.FailClosedDeny)
    }

    @Test
    fun `unspecified fail mode is open`() {
        val mock = MockDecider(error = RuntimeException("down"))
        val spec = makeSpec("spec-1", listOf(testKind), FailMode.FAIL_MODE_UNSPECIFIED, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK)
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), spec), i64(10000), makeDeps(mock, 0, alwaysClosed), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit, "an undeclared fail mode is OPEN")
    }

    @Test
    fun `ship async specs are permitted without asking`() {
        val mock = MockDecider()
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), shipAsyncSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Permit)
        assertEquals(0, mock.callCount(), "ship-async never asks")
        assertEquals(testEpoch, outcome.filterEpoch, "the epoch is audited even on a no-op")
    }

    @Test
    fun `non selecting spec is not enforcing`() {
        val mock = MockDecider()
        val other = makeSpec("spec-1", listOf("approval.granted"), FailMode.FAIL_MODE_CLOSED, DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK)
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), other), i64(10000), makeDeps(mock, 0, alwaysClosed), testOptions)
        assertTrue(outcome is GateOutcome.Permit)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `budget exhausted skips decide`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        val outcome =
            gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 10000), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals("budget-exhausted", outcome.reason)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `budget already past deadline skips decide`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        val outcome =
            gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 10001), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `unspecified action is not a blind permit`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_UNSPECIFIED))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
    }

    @Test
    fun `unspecified action fails closed when contracted`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_UNSPECIFIED))
        val outcome =
            gate(
                makeEvent(testKind),
                makeFilter(u64(testEpoch), closedAskAndBlockSpec()),
                i64(10000),
                makeDeps(mock, 0, alwaysClosed),
                testOptions,
            )
        assertTrue(outcome is GateOutcome.FailClosedDeny)
    }

    @Test
    fun `unknown action is not a blind permit`() {
        // A future DecisionAction this Probe does not understand is unresolved, not permitted.
        // Java protobuf enums throw on UNRECOGNIZED, so construct the response via raw bytes:
        // field 2 (action) varint value 99, which is not a known DecisionAction.
        val unknownActionResponse =
            DecideResponse.parseFrom(
                com.google.protobuf.ByteString
                    .copyFrom(byteArrayOf(0x10, 0x63)),
            )
        val mock = MockDecider(response = unknownActionResponse)
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
    }

    @Test
    fun `budget comes from the injected clock`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 5000), testOptions)
        val request = mock.lastRequest()!!
        assertTrue(request.hasRemainingTransportBudgetNanoseconds())
        assertEquals(5000L, request.remainingTransportBudgetNanoseconds)
    }

    @Test
    fun `without caller budget uses Specification budget`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), null, makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Permit)
        assertEquals(10_000L, mock.lastRequest()!!.remainingTransportBudgetNanoseconds)
    }

    @Test
    fun `without caller budget defers while Specification budget remains`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DEFER))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), null, makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Defer)
    }

    @Test
    fun `without budget transport error fails open`() {
        val mock = MockDecider(error = RuntimeException("timeout"))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), null, makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
    }

    @Test
    fun `no filter held`() {
        val mock = MockDecider()
        val outcome = gate(makeEvent(testKind), null, i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.NoFilter)
        assertNull(outcome.filterEpoch)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `filter without epoch is no filter`() {
        val mock = MockDecider()
        val outcome = gate(makeEvent(testKind), makeFilter(null, askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.NoFilter)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `epoch zero is a filter`() {
        // The epoch-0 trap at the gate: getEpoch() == 0 would silently stop enforcing for every
        // source on epoch 0. Presence is `hasEpoch`, so epoch 0 is enforced.
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DENY))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(0), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Deny, "epoch 0 is a held filter and must be enforced")
        assertEquals(0L, outcome.filterEpoch, "FilterEpoch must be a present 0")
        assertTrue(mock.lastRequest()!!.hasFilterEpoch(), "filter_epoch 0 must be present on the wire")
    }

    @Test
    fun `request carries identifiers and event`() {
        val mock = MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT))
        val event =
            ProducerEvent
                .newBuilder()
                .setId("evt-9")
                .setKind(testKind)
                .setSchemaVersion("v1")
                .build()
        gate(event, makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        val request = mock.lastRequest()!!
        assertEquals("req-1", request.requestId)
        assertEquals("idem-1", request.idempotencyKey)
        assertEquals("gateway.tool-calls", request.sourceHandle)
        assertEquals("evt-9", request.producerEvent.id)
        assertEquals(testEpoch, request.filterEpoch)
    }

    @Test
    fun `audits filter epoch in every outcome`() {
        val epoch = 42L

        data class Case(
            val name: String,
            val mock: MockDecider,
            val wantKind: kotlin.reflect.KClass<out GateOutcome>,
        )
        val cases =
            listOf(
                Case("permit", MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_PERMIT)), GateOutcome.Permit::class),
                Case("deny", MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DENY)), GateOutcome.Deny::class),
                Case("defer", MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_DEFER)), GateOutcome.Defer::class),
                Case(
                    "unspecified",
                    MockDecider(response = makeResponse(DecisionAction.DECISION_ACTION_UNSPECIFIED)),
                    GateOutcome.FailOpenPermit::class,
                ),
                Case("transport error", MockDecider(error = RuntimeException("boom")), GateOutcome.FailOpenPermit::class),
            )
        for (c in cases) {
            val outcome = gate(makeEvent(testKind), makeFilter(epoch, askAndBlockSpec()), i64(10000), makeDeps(c.mock, 0), testOptions)
            assertEquals(epoch, outcome.filterEpoch, "${c.name}: FilterEpoch must be audited")
            assertTrue(outcome.reason.isNotEmpty(), "${c.name}: Reason must be populated (D15)")
            assertEquals(c.wantKind, outcome::class, c.name)
        }
    }

    @Test
    fun `surfaces specification decisions`() {
        val reason = UnresolvedReason.UNRESOLVED_REASON_EVIDENCE_GAP
        val mock =
            MockDecider(
                response =
                    makeResponse(
                        DecisionAction.DECISION_ACTION_DEFER,
                        makeDecision("spec-1", DecisionAction.DECISION_ACTION_DEFER, reason),
                    ),
            )
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.Defer)
        assertEquals(1, outcome.specifications.size)
        val decision = outcome.specifications[0]
        assertEquals("spec-1", decision.specificationId)
        assertEquals(UnresolvedReason.UNRESOLVED_REASON_EVIDENCE_GAP, decision.unresolvedReason)
    }

    @Test
    fun `surfaces specification decisions on a fail-mode outcome`() {
        val reason = UnresolvedReason.UNRESOLVED_REASON_TIMEOUT
        val mock =
            MockDecider(
                response =
                    makeResponse(
                        DecisionAction.DECISION_ACTION_UNSPECIFIED,
                        makeDecision("spec-1", DecisionAction.DECISION_ACTION_UNSPECIFIED, reason),
                    ),
            )
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals(1, outcome.specifications.size)
        assertEquals(UnresolvedReason.UNRESOLVED_REASON_TIMEOUT, outcome.specifications[0].unresolvedReason)
    }

    @Test
    fun `null response applies the fail mode, never a blind permit`() {
        // Null guard: a transport that yielded nothing is unresolved, never a permit. The mock
        // returns Ok(null) to model a transport that came back empty-handed.
        val mock = MockDecider(response = null)
        val deps = Deps(decide = { DecideResult.Ok(null) }, nowMonotonicNs = { 0L }, acceptedFailModeFor = alwaysOpen)
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), deps, testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals("unspecified-action", outcome.reason)
    }

    @Test
    fun `classifies a Connect error distinctly from a transport error`() {
        val mock = MockDecider(error = ConnectError("unavailable", "endpoint down"))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), i64(10000), makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertTrue(outcome.reason.startsWith("connect-unavailable"), "reason = ${outcome.reason}")
    }

    @Test
    fun `classifies a timeout as a context deadline`() {
        val mock = MockDecider(error = TimeoutException("deadline"))
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), askAndBlockSpec()), null, makeDeps(mock, 0), testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertTrue(outcome.reason.startsWith("context-deadline-exceeded"), "reason = ${outcome.reason}")
    }

    @Test
    fun `noFilter does not need dependencies`() {
        val mock = MockDecider()
        val deps = Deps(decide = mock::decide, nowMonotonicNs = { 0L }, acceptedFailModeFor = null)
        val outcome = gate(makeEvent(testKind), null, i64(10000), deps, testOptions)
        assertTrue(outcome is GateOutcome.NoFilter)
        assertEquals(0, mock.callCount())
    }

    @Test
    fun `missing eligible budget exhausts without clock or Decide`() {
        val spec = askAndBlockSpec().toBuilder().clearLatencyBudgetNanoseconds().build()
        val mock = MockDecider()
        var clockCalls = 0
        val deps =
            Deps(
                decide = mock::decide,
                nowMonotonicNs = {
                    clockCalls++
                    0L
                },
                acceptedFailModeFor = { FailMode.FAIL_MODE_OPEN },
            )
        val outcome = gate(makeEvent(testKind), makeFilter(u64(testEpoch), spec), null, deps, testOptions)
        assertTrue(outcome is GateOutcome.FailOpenPermit)
        assertEquals(0, clockCalls)
        assertEquals(0, mock.callCount())
    }
}
