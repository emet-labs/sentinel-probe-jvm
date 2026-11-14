package dev.emet.sentinel.probe.sdk.client

import dev.emet.sentinel.probe.sdk.otlp.OtlpLogReceiver
import dev.emet.sentinel.probe.v1.DecideRequest

// ProbeClient owns the Probe's filter state and builds decision requests against it.
// Kotlin analog of sdk/go/client/client.go.
//
// Safe for concurrent use: all mutable state lives behind FilterStore's atomic reference.
public class ProbeClient(
    // config carries the Probe's identity and the Sentinel it talks to.
    public val config: Config,
    // decider returns the DecideResponse the OTLP log receiver decoded for this request.
    // In tests it is a stub. It is a REQUIRED parameter with no default: a silently-defaulted
    // transport would hide a missing log until the first enforcement call. Connect is not
    // a production decider.
    private val decider: (DecideRequest) -> dev.emet.sentinel.probe.sdk.enforcement.DecideResult,
) {
    private val store: FilterStore = FilterStore.newFilterStore(config.initialFilter)

    // Config describes the Probe's identity and the Sentinel it talks to. Analog of
    // probe-client.ts's ProbeClientConfig.
    public data class Config(
        // sourceHandle is the Probe's source_handle, used in DecideRequest.source_handle.
        public val sourceHandle: String,
        // sentinelBaseUrl is the Sentinel decision endpoint base URL. The SDK never reads it —
        // the decider passed to the constructor is what carries requests, and it already holds
        // its own base URL. The field exists for parity with the reference's
        // ProbeClientConfig.sentinelBaseUrl, which is equally inert there, and so a host can
        // keep its endpoint configuration in one struct.
        public val sentinelBaseUrl: String,
        // initialFilter optionally seeds the store before the first push, for example from a
        // local cache.
        public val initialFilter: dev.emet.sentinel.model.v1.EventFilter? = null,
    )

    // currentFilter returns the EventFilter for this source, or null before the first refresh.
    // The reference is stable until the next setFilter.
    public fun currentFilter(): dev.emet.sentinel.model.v1.EventFilter? = store.get()

    // acknowledgedEpoch returns the held filter epoch, the value a Probe stamps into
    // ProducerEvent.acknowledged_filter_epoch. null means no epoch is held; it does not mean 0.
    public fun acknowledgedEpoch(): Long? = store.epoch()

    // setFilter swaps in a new EventFilter, reporting whether the store was actually updated.
    // Only a first filter or a strictly newer epoch updates it; an equal epoch is a no-op and an
    // older or epoch-less filter is refused (see FilterStore.set).
    //
    // Where the new filter comes from is out of SDK scope: in v1 Sentinel pushes filters
    // (ADR-0006) and no push RPC exists in decision.proto, so the host calls setFilter when a
    // push arrives. Same division of labour as the TypeScript reference.
    public fun setFilter(filter: dev.emet.sentinel.model.v1.EventFilter?): Boolean = store.set(filter)

    // refreshOnEpoch reports whether an announced epoch warrants fetching a new filter.
    public fun refreshOnEpoch(newEpoch: Long?): Boolean = store.shouldRefresh(newEpoch)

    // buildDecideRequest builds a DecideRequest against the held filter epoch.
    //
    // The event must already have been projected by ApplyFilter; this method does not project,
    // exactly as the TypeScript reference does not. remainingBudgetNs is null when the caller
    // set no latency budget.
    public fun buildDecideRequest(
        event: dev.emet.sentinel.model.v1.ProducerEvent,
        requestID: String,
        idempotencyKey: String,
        remainingBudgetNs: Long?,
    ): DecideRequest {
        val builder =
            DecideRequest
                .newBuilder()
                .setRequestId(requestID)
                .setIdempotencyKey(idempotencyKey)
                .setSourceHandle(config.sourceHandle)
                .setProducerEvent(event)
        val epoch = store.epoch()
        if (epoch != null) builder.setFilterEpoch(epoch)
        if (remainingBudgetNs != null) builder.setRemainingTransportBudgetNanoseconds(remainingBudgetNs)
        return builder.build()
    }

    // decider exposes the decide adapter so the enforcement gate can call it.
    public fun decider(): (DecideRequest) -> dev.emet.sentinel.probe.sdk.enforcement.DecideResult = decider

    // sourceHandle returns the configured source handle, which the enforcement gate stamps into
    // every DecideRequest.
    public fun sourceHandle(): String = config.sourceHandle

    // decideFunc exposes the decide dependency the enforcement gate's Deps.decide expects, so a
    // host does not have to adapt the decider by hand.
    public fun decideFunc(): (DecideRequest) -> dev.emet.sentinel.probe.sdk.enforcement.DecideResult = decider

    public companion object {
        // fromLogReceiver wires the OTLP log receiver as the only decision path and applies
        // EventFilter logs to this client. Decide is not called.
        @JvmStatic
        public fun fromLogReceiver(
            config: Config,
            receiver: OtlpLogReceiver,
        ): ProbeClient {
            val client = ProbeClient(config, receiver::decide)
            receiver.addFilterListener { filter ->
                client.setFilter(filter)
            }
            return client
        }
    }
}
