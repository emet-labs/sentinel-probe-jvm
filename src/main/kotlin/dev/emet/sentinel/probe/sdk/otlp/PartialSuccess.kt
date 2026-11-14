// PartialSuccess reads the ExportTraceServiceResponse Sentinel returns for a trace export.
//
// An export succeeded only on HTTP 2xx with partial_success.rejected_spans == 0; an absent
// partial_success counts as 0. Sentinel counts conflicts, admission refusals and Events above a
// closed boundary as rejected, so a rejected span is an Event Sentinel does not hold. A
// duplicate is reported with zero rejected spans and a message. A body that does not decode is
// a failure, never a success.
package dev.emet.sentinel.probe.sdk.otlp

import com.google.protobuf.InvalidProtocolBufferException
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse

// Rejection is Sentinel's partial_success: how many spans it rejected and its error_message.
public data class Rejection(
    public val rejectedSpans: Long,
    public val errorMessage: String,
) {
    // isDuplicateEventId is true when the message names evidence_conflict or identity_conflict:
    // Sentinel already holds an Event under that id.
    public val isDuplicateEventId: Boolean
        get() = rejectedSpans > 0 && (errorMessage.contains("evidence_conflict") || errorMessage.contains("identity_conflict"))
}

public object PartialSuccess {
    // rejectedSpans returns the rejected span count of a 2xx trace export body. It throws
    // IllegalArgumentException for a body that is not a valid response.
    public fun rejectedSpans(body: ByteArray): Long = rejection(body).rejectedSpans

    // rejection returns the rejected count and error_message of a 2xx trace export body.
    public fun rejection(body: ByteArray): Rejection {
        val response =
            try {
                ExportTraceServiceResponse.parseFrom(body)
            } catch (error: InvalidProtocolBufferException) {
                throw IllegalArgumentException("ExportTraceServiceResponse is not valid protobuf: ${error.message}", error)
            }
        val rejected = response.partialSuccess.rejectedSpans
        require(rejected >= 0) { "ExportTraceServiceResponse rejected_spans is negative: $rejected" }
        return Rejection(rejected, response.partialSuccess.errorMessage)
    }
}
