package dev.emet.sentinel.probe.sdk.emission

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ParentEventIdTests {
    @Test
    fun `inject writes the header and extract reads it back, case-insensitively`() {
        val headers = mutableMapOf("X-Sentinel-Parent-Event-Id" to "stale")
        assertTrue(injectParentEventId(headers, "evt-1"))
        assertEquals(mapOf("x-sentinel-parent-event-id" to "evt-1"), headers)
        assertEquals("evt-1", extractParentEventId(mapOf("X-SENTINEL-PARENT-EVENT-ID" to " evt-1 ")))
    }

    @Test
    fun `extract falls back to the tracestate sentinel member`() {
        val headers = mapOf("tracestate" to "vendor=a, sentinel=evt-9 ,other=b")
        assertEquals("evt-9", extractParentEventId(headers))
    }

    @Test
    fun `the header wins over tracestate, and a blank header falls back`() {
        val both = mapOf("x-sentinel-parent-event-id" to "from-header", "tracestate" to "sentinel=from-state")
        assertEquals("from-header", extractParentEventId(both))
        assertEquals("from-state", extractParentEventId(mapOf("x-sentinel-parent-event-id" to "  ", "tracestate" to "sentinel=from-state")))
    }

    @Test
    fun `extract is null when nothing usable travels`() {
        assertNull(extractParentEventId(emptyMap()))
        assertNull(extractParentEventId(mapOf("tracestate" to "vendor=a,sentinelx=b,=c,sentinel=")))
    }

    @Test
    fun `inject refuses a blank id or a control character and writes nothing`() {
        val headers = mutableMapOf<String, String>()
        assertFalse(injectParentEventId(headers, null))
        assertFalse(injectParentEventId(headers, "  "))
        assertFalse(injectParentEventId(headers, "a\r\nx-evil: 1"))
        assertTrue(headers.isEmpty())
    }

    @Test
    fun `getter and setter forms serve any carrier`() {
        class Request(
            val headers: MutableList<Pair<String, String>> = ArrayList(),
        )

        val request = Request()
        assertTrue(injectParentEventId(request, "evt-3") { r, name, value -> r.headers.add(name to value) })
        assertEquals(listOf("x-sentinel-parent-event-id" to "evt-3"), request.headers)
        val read = extractParentEventId(request) { r, name -> r.headers.firstOrNull { it.first == name }?.second }
        assertEquals("evt-3", read)
    }
}
