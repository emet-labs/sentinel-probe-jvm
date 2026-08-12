// Package enforcement implements the ask-and-block gate. Kotlin analog of
// sdk/go/enforcement/{gate,budget}.go and sdk/typescript/src/enforcement/gate.ts.
package dev.emet.sentinel.probe.sdk.enforcement

import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SpecificationFilter
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.SpecificationDecision

// remainingTransportBudgetNs computes the remaining transport budget from a monotonic
// absolute deadline. Analog of Go's RemainingTransportBudgetNs / monotonic-budget.ts.
//
// deadlineNs is a monotonic ABSOLUTE, computed by the caller at gate entry as
// nowMonotonicNs + latency_budget_ns. nowNs is the current monotonic reading, injected.
// A passed deadline yields 0, never a negative budget.
//
// Pure: no clock access, no side effects. The clock is injected through Deps.nowMonotonicNs
// and never read inside, so budget behaviour is testable without sleeping.
public fun remainingTransportBudgetNs(deadlineNs: Long, nowNs: Long): Long {
    val remaining = deadlineNs - nowNs
    return if (remaining <= 0) 0L else remaining
}

// GateOutcome is what the Probe must do, plus the evidence for why. A sealed class is the
// direct sum-type analog of the TypeScript GateOutcome discriminated union that Go's struct
// could not express (divergence D11/D15 note in the Go reference). Every outcome carries a
// reason and the filter epoch it was taken against, so an audit record is never ambiguous.
public sealed class GateOutcome {
    public abstract val reason: String
    public abstract val filterEpoch: Long?
    public abstract val specifications: List<SpecificationDecision>

    // permitted reports whether the Probe may proceed. Permit and fail-open-permit allow the
    // action; deny, fail-closed-deny and defer do not. NoFilter permits, matching the
    // reference's conservative default of not blocking on absent policy.
    public val permitted: Boolean
        get() = this is Permit || this is FailOpenPermit || this is NoFilter

    public data class Permit(
        override val reason: String,
        override val filterEpoch: Long?,
        override val specifications: List<SpecificationDecision> = emptyList(),
    ) : GateOutcome()

    public data class Deny(
        override val reason: String,
        override val filterEpoch: Long?,
        override val specifications: List<SpecificationDecision> = emptyList(),
    ) : GateOutcome()

    public data class Defer(
        override val reason: String,
        override val filterEpoch: Long?,
        override val specifications: List<SpecificationDecision> = emptyList(),
    ) : GateOutcome()

    public data class FailOpenPermit(
        override val reason: String,
        override val filterEpoch: Long?,
        override val specifications: List<SpecificationDecision> = emptyList(),
    ) : GateOutcome()

    public data class FailClosedDeny(
        override val reason: String,
        override val filterEpoch: Long?,
        override val specifications: List<SpecificationDecision> = emptyList(),
    ) : GateOutcome()

    // NoFilter means no filter or no filter epoch is held, so there is nothing to enforce and
    // no ask is made. A discriminant of its own rather than a fail-open, so an auditor can tell
    // "we had no policy" from "we had policy and could not reach Sentinel".
    public data class NoFilter(
        override val reason: String,
    ) : GateOutcome() {
        override val filterEpoch: Long? get() = null
        override val specifications: List<SpecificationDecision> get() = emptyList()
    }
}

// Deps are the effects the gate needs, all injected so the gate itself stays pure and testable
// without a network or a clock.
public data class Deps(
    // decide performs the ask. In production this wraps SentinelTransport.decide; in tests it
    // is a stub. Returns null for the response to model a transport that yielded nothing.
    public val decide: (request: DecideRequest) -> DecideResult,
    // nowMonotonicNs reads the host's monotonic clock. Required whenever deadlineNs is set.
    public val nowMonotonicNs: () -> Long,
    // acceptedFailModeFor reports the fail mode the deployment has actually contracted for a
    // spec. A spec declaring CLOSED without an accepted contract downgrades to OPEN: an
    // operator must have agreed to be blocked.
    //
    // REQUIRED whenever an enforcing Specification selects the event. Gate throws when it is
    // null, exactly as it already would on a null decide or a null nowMonotonicNs with a
    // deadline set. Treating null as "nothing accepted" would make this the one dependency
    // whose absence silently WEAKENS enforcement.
    public val acceptedFailModeFor: ((spec: SpecificationFilter) -> FailMode)?,
)

// DecideResult is the (response, error) shape the decide dependency returns. A null response
// with no error models a transport that yielded nothing — the Go nil-safe guard applies the
// fail mode rather than permit. Kotlin has no multi-return; a sealed result is the honest shape.
public sealed class DecideResult {
    public data class Ok(val response: DecideResponse?) : DecideResult()
    public data class Err(val error: Throwable) : DecideResult()
}

// Options carry the per-call identifiers stamped into the DecideRequest.
public data class Options(
    public val sourceHandle: String,
    public val requestID: String,
    public val idempotencyKey: String,
)

// gate enforces one ASK_AND_BLOCK event and returns the action the Probe must take.
//
// Control flow mirrors enforcement-gate.ts:57-201 step for step:
//
//  1. no filter, or a filter with no epoch, returns NoFilter without asking;
//  2. the enforcing set is the specs that select the event AND declare ASK_AND_BLOCK
//     delivery; an empty set means the event is ship-async and the gate is a no-op permit;
//  3. the aggregate fail mode is CLOSED iff some enforcing spec declares CLOSED and the
//     deployment has accepted CLOSED for it — fail-closed wins over any number of open specs;
//  4. if a deadline was set and the budget is already exhausted, apply the fail mode WITHOUT
//     asking;
//  5. build the DecideRequest from the event the caller passed, which the caller has already
//     projected through ApplyFilter — gate never projects;
//  6. ask; any error applies the aggregate fail mode; a null response applies the fail mode
//     (never a blind permit);
//  7. PERMIT permits, DENY denies, DEFER defers while budget remains or no deadline was set and
//     otherwise applies the fail mode, and UNSPECIFIED applies the fail mode — never a blind
//     permit.
public fun gate(
    event: ProducerEvent,
    filter: dev.emet.sentinel.model.v1.EventFilter?,
    deadlineNs: Long?,
    deps: Deps,
    options: Options,
): GateOutcome {
    // 1. No-filter guard. Presence is `!hasEpoch`: epoch 0 is a legitimate epoch, so
    //    getEpoch() == 0 would misclassify it as "no policy held".
    if (filter == null || !filter.hasEpoch()) return GateOutcome.NoFilter(reason = "no-filter")
    val filterEpoch: Long = filter.epoch

    // 2. Enforcing set: selects the event AND asks-and-blocks.
    val enforcing = filter.specificationsList.filter { spec ->
        dev.emet.sentinel.probe.sdk.internal.specmatch.SpecMatch.selects(spec, event) &&
            isAskAndBlock(spec)
    }
    if (enforcing.isEmpty()) {
        return GateOutcome.Permit(
            reason = "no-ask-and-block-spec",
            filterEpoch = filterEpoch,
        )
    }

    // 3. Aggregate fail mode: fail-closed wins.
    val aggregateFailMode = computeAggregateFailMode(enforcing, deps)

    // 4. Budget. Exhausted before the call means apply the fail mode without asking, so a
    //    Probe that is already out of time does not spend more of it.
    var remainingBudget: Long? = null
    if (deadlineNs != null) {
        val remaining = remainingTransportBudgetNs(deadlineNs, deps.nowMonotonicNs())
        if (remaining == 0L) {
            return applyFailMode(aggregateFailMode, "budget-exhausted", filterEpoch, null)
        }
        remainingBudget = remaining
    }

    // 5. Build the request from the already-projected event.
    val requestBuilder = DecideRequest.newBuilder()
        .setRequestId(options.requestID)
        .setIdempotencyKey(options.idempotencyKey)
        .setSourceHandle(options.sourceHandle)
        .setFilterEpoch(filterEpoch)
        .setProducerEvent(event)
    if (remainingBudget != null) requestBuilder.setRemainingTransportBudgetNanoseconds(remainingBudget)
    val request = requestBuilder.build()

    // 6. Ask. Never a permit, never a blind pass-through.
    val result = deps.decide(request)
    return when (result) {
        is DecideResult.Err -> applyFailMode(aggregateFailMode, describeError(result.error), filterEpoch, null)
        is DecideResult.Ok -> handleResponse(result.response, aggregateFailMode, filterEpoch, deadlineNs, deps)
    }
}

private fun handleResponse(
    response: DecideResponse?,
    aggregateFailMode: FailMode,
    filterEpoch: Long,
    deadlineNs: Long?,
    deps: Deps,
): GateOutcome {
    val decisions = response?.specificationsList ?: emptyList()
    if (response == null) {
        // Null guard: a transport that yielded nothing is unresolved, never a permit — matches
        // the Go nil-safe path that applies the fail mode with an "unspecified-action" reason.
        return applyFailMode(aggregateFailMode, "unspecified-action", filterEpoch, decisions)
    }
    return when (response.action) {
        dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_PERMIT ->
            GateOutcome.Permit(reason = "permit", filterEpoch = filterEpoch, specifications = decisions)
        dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DENY ->
            GateOutcome.Deny(reason = "deny", filterEpoch = filterEpoch, specifications = decisions)
        dev.emet.sentinel.probe.v1.DecisionAction.DECISION_ACTION_DEFER -> {
            if (deadlineNs == null) {
                // No latency budget was declared, so there is no timeout path: defer indefinitely.
                GateOutcome.Defer(reason = "defer", filterEpoch = filterEpoch, specifications = decisions)
            } else if (remainingTransportBudgetNs(deadlineNs, deps.nowMonotonicNs()) > 0L) {
                GateOutcome.Defer(reason = "defer", filterEpoch = filterEpoch, specifications = decisions)
            } else {
                applyFailMode(aggregateFailMode, "defer-budget-exhausted", filterEpoch, decisions)
            }
        }
        // UNSPECIFIED and any unknown future action are unresolved, not permitted.
        else -> applyFailMode(aggregateFailMode, "unspecified-action", filterEpoch, decisions)
    }
}

private fun applyFailMode(
    failMode: FailMode,
    reason: String,
    filterEpoch: Long,
    decisions: List<SpecificationDecision>?,
): GateOutcome {
    val specs = decisions ?: emptyList()
    return if (failMode == FailMode.FAIL_MODE_CLOSED) {
        GateOutcome.FailClosedDeny(reason = reason, filterEpoch = filterEpoch, specifications = specs)
    } else {
        GateOutcome.FailOpenPermit(reason = reason, filterEpoch = filterEpoch, specifications = specs)
    }
}

// computeAggregateFailMode returns CLOSED iff some enforcing spec both DECLARES CLOSED and has
// CLOSED accepted by the deployment. Declaration alone downgrades to OPEN: blocking a caller is
// something an operator has to have agreed to. FAIL_MODE_UNSPECIFIED is OPEN.
//
// Throws on a null Deps.acceptedFailModeFor. A missing contract source is a wiring bug, and
// defaulting it to OPEN would silently disable fail-closed for every Specification — the exact
// failure this package exists to prevent. It is only reached when the enforcing set is
// non-empty, so a Probe with no ask-and-block Specifications never needs the dependency.
private fun computeAggregateFailMode(
    enforcing: List<SpecificationFilter>,
    deps: Deps,
): FailMode {
    val accepted = deps.acceptedFailModeFor
        ?: throw IllegalStateException(
            "enforcement: Deps.acceptedFailModeFor is required when an enforcing " +
                "Specification selects the event; a null contract source cannot be defaulted to " +
                "fail-open without silently disabling fail-closed enforcement",
        )
    for (spec in enforcing) {
        if (spec.failMode != FailMode.FAIL_MODE_CLOSED) continue
        if (accepted(spec) == FailMode.FAIL_MODE_CLOSED) return FailMode.FAIL_MODE_CLOSED
    }
    return FailMode.FAIL_MODE_OPEN
}

private fun isAskAndBlock(spec: SpecificationFilter): Boolean =
    spec.eventMatch.deliveryMode == dev.emet.sentinel.model.v1.DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK

// CodedError marks a transport error that carries a Connect code (e.g. "unavailable"). The
// transport's ConnectError implements it; enforcement depends on the interface, not the
// concrete class, so the enforcement package never imports the client package — mirroring
// the Go layout where both import the shared connectrpc library rather than each other.
public interface CodedError {
    public val code: String
}

// describeError renders a transport failure for the audit record, distinguishing an interrupted
// transport from a Connect status so an operator can tell "the host gave up" from "the endpoint
// said no". Every error class still routes into the fail mode.
public fun describeError(error: Throwable): String {
    // A Connect error carries a code; surface it distinctly from a raw transport error. The
    // concrete ConnectError formats its message as "connect-<code>: <message>", which an audit
    // record can read directly.
    if (error is CodedError) {
        return error.message ?: "connect-${error.code}"
    }
    val name = error.javaClass.simpleName
    return when {
        name.contains("Timeout", ignoreCase = true) ||
            error is java.io.InterruptedIOException ->
            "context-deadline-exceeded: ${error.message ?: name}"
        else -> "transport-error: ${error.message ?: name}"
    }
}
