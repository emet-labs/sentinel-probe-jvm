package dev.emet.sentinel.probe.sdk.internal.gensmoke

import com.google.protobuf.ByteString
import dev.emet.sentinel.model.v1.AttributeEntry
import dev.emet.sentinel.model.v1.AttributeValue
import dev.emet.sentinel.model.v1.DeliveryMode
import dev.emet.sentinel.model.v1.EventFilter
import dev.emet.sentinel.model.v1.FailMode
import dev.emet.sentinel.model.v1.ProducerEvent
import dev.emet.sentinel.model.v1.SourceCapability
import dev.emet.sentinel.model.v1.SourceTier
import dev.emet.sentinel.probe.v1.DecisionAction
import dev.emet.sentinel.probe.v1.DecideRequest
import dev.emet.sentinel.probe.v1.DecideResponse
import dev.emet.sentinel.probe.v1.SpecificationDecision
import dev.emet.sentinel.probe.v1.UnresolvedReason
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

// CodegenSmokeTests is the analog of sdk/go/internal/gensmoke/gensmoke_test.go. It has no
// production surface: its job is to fail loudly and early if the generated gen/ tree stops
// matching what the hand-written Kotlin assumes — the Java package prefix, the proto-prefixed
// enum constant names protoc-gen-java emits, and the `optional` presence semantics the
// enforcement gate depends on.
class CodegenSmokeTests {
    @Test
    fun `generated messages round-trip through binary protobuf`() {
        val epoch = 7L
        val req = DecideRequest.newBuilder()
            .setRequestId("req-9")
            .setIdempotencyKey("idem-9")
            .setSourceHandle("gateway.tool-calls")
            .setFilterEpoch(epoch)
            .setProducerEvent(
                ProducerEvent.newBuilder()
                    .setId("evt-9")
                    .setKind("transfer.initiated")
                    .setSchemaVersion("sentinel.model.v1")
                    .addAttributes(
                        AttributeEntry.newBuilder()
                            .setKey("sagashop.order.id")
                            .setValue(AttributeValue.newBuilder().setStringValue("ord-9").build())
                            .build(),
                    )
                    .build(),
            )
            .setRemainingTransportBudgetNanoseconds(1234L)
            .build()

        val wire = req.toByteArray()
        val got = DecideRequest.parseFrom(wire)
        assertEquals(req, got, "binary protobuf round-trip mismatch")
        assertEquals("ord-9", got.producerEvent.attributesList[0].value.stringValue, "nested attribute must survive")
        assertEquals(epoch, got.filterEpoch)
    }

    @Test
    fun `generated enums keep the proto prefix`() {
        // protoc-gen-java does NOT strip enum prefixes (unlike bufbuild/es). Transliterating the
        // TypeScript form would not compile, and a silent renumbering would change the wire
        // contract, so both name and number are asserted.
        assertEquals(FailMode.FAIL_MODE_UNSPECIFIED, FailMode.forNumber(0))
        assertEquals(FailMode.FAIL_MODE_OPEN, FailMode.forNumber(1))
        assertEquals(FailMode.FAIL_MODE_CLOSED, FailMode.forNumber(2))
        assertEquals(2, FailMode.FAIL_MODE_CLOSED.number)

        assertEquals(DeliveryMode.DELIVERY_MODE_SHIP_ASYNC, DeliveryMode.forNumber(1))
        assertEquals(DeliveryMode.DELIVERY_MODE_ASK_AND_BLOCK, DeliveryMode.forNumber(2))

        assertEquals(SourceTier.SOURCE_TIER_ANCHOR, SourceTier.forNumber(1))
        assertEquals(SourceTier.SOURCE_TIER_CONTRIBUTING, SourceTier.forNumber(2))
        assertEquals(SourceCapability.SOURCE_CAPABILITY_CAUSAL_EDGES, SourceCapability.forNumber(2))

        assertEquals(DecisionAction.DECISION_ACTION_PERMIT, DecisionAction.forNumber(1))
        assertEquals(DecisionAction.DECISION_ACTION_DEFER, DecisionAction.forNumber(3))
        assertEquals(UnresolvedReason.UNRESOLVED_REASON_EVIDENCE_GAP, UnresolvedReason.forNumber(4))
    }

    @Test
    fun `optional epoch presence is hasEpoch, not getEpoch == 0`() {
        // EventFilter.epoch is `optional uint64`, so protoc-gen-java emits a hasEpoch() presence
        // check plus a getEpoch() that returns 0 for the absent case — and 0 is a legitimate
        // epoch. Every presence check in this SDK must be !hasEpoch, never getEpoch() == 0.
        val withEpoch = EventFilter.newBuilder().setEpoch(0L).build()
        assertTrue(withEpoch.hasEpoch(), "epoch 0 is present")
        assertEquals(0L, withEpoch.epoch)

        val withoutEpoch = EventFilter.newBuilder().build()
        assertFalse(withoutEpoch.hasEpoch(), "absent epoch is not present")
        assertEquals(0L, withoutEpoch.epoch, "getEpoch flattens absent to 0 — presence must be hasEpoch")
    }

    @Test
    fun `decide response carries specification decisions with unresolved reasons`() {
        val reason = UnresolvedReason.UNRESOLVED_REASON_TIMEOUT
        val resp = DecideResponse.newBuilder()
            .setRequestId("r")
            .setAction(DecisionAction.DECISION_ACTION_DEFER)
            .addSpecifications(
                SpecificationDecision.newBuilder()
                    .setSpecificationId("spec-1")
                    .setSpecificationVersion("1.0.0")
                    .setAction(DecisionAction.DECISION_ACTION_DEFER)
                    .setFailMode(FailMode.FAIL_MODE_CLOSED)
                    .setUnresolvedReason(reason)
                    .build(),
            )
            .build()
        val round = DecideResponse.parseFrom(resp.toByteArray())
        assertEquals(1, round.specificationsList.size)
        val decision = round.specificationsList[0]
        assertEquals("spec-1", decision.specificationId)
        assertEquals(reason, decision.unresolvedReason)
        assertEquals(FailMode.FAIL_MODE_CLOSED, decision.failMode)
    }

    @Test
    fun `byteString round-trips through bytes_value`() {
        // Pins that ByteString maps cleanly into the bytes_value oneof arm even though OTel Java
        // attributes never reach it: a host that builds an AttributeValue directly still needs the
        // arm to be a working protobuf field.
        val payload = ByteString.copyFrom(byteArrayOf(0xde.toByte(), 0xad.toByte(), 0xbe.toByte(), 0xef.toByte()))
        val v = AttributeValue.newBuilder().setBytesValue(payload).build()
        val round = AttributeValue.parseFrom(v.toByteArray())
        assertEquals(AttributeValue.ValueCase.BYTES_VALUE, round.valueCase)
        assertEquals(payload, round.bytesValue)
    }
}
