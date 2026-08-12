package dev.emet.sentinel.probe.sdk.ids

import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class IdsTests {
    @Test
    fun `generated identifiers are distinct UUIDv4 strings`() {
        val seen = HashSet<String>()
        repeat(128) {
            for (id in listOf(ProbeIds.generateRequestID(), ProbeIds.generateIdempotencyKey())) {
                val parsed = UUID.fromString(id)
                assertEquals(4, parsed.version(), "$id is not UUID version 4")
                assertTrue(id !in seen, "duplicate identifier $id")
                seen.add(id)
            }
        }
    }

    @Test
    fun `request id and idempotency key are independent`() {
        // A distinct function even though the implementation is identical: collapsing them would
        // make retries indistinguishable from fresh asks at the receiver.
        assertNotEquals(ProbeIds.generateRequestID(), ProbeIds.generateIdempotencyKey())
    }
}
