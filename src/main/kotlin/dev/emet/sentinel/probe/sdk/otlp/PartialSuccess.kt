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

public object PartialSuccess {
    // rejectedSpans returns the rejected span count of a 2xx trace export body. It throws
    // IllegalArgumentException for a body that is not a valid response.
    public fun rejectedSpans(body: ByteArray): Long {
        val response =
            try {
                ExportTraceServiceResponse.parseFrom(body)
            } catch (error: InvalidProtocolBufferException) {
                throw IllegalArgumentException("ExportTraceServiceResponse is not valid protobuf: ${error.message}", error)
            }
        val rejected = response.partialSuccess.rejectedSpans
        require(rejected >= 0) { "ExportTraceServiceResponse rejected_spans is negative: $rejected" }
        return rejected
    }
}
