package dev.emet.sentinel.probe.sdk.enforcement

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class BudgetTests {
    @Test
    fun `remaining budget is deadline minus now`() {
        assertEquals(5000L, remainingTransportBudgetNs(10000, 5000))
    }

    @Test
    fun `a passed deadline yields zero, never negative`() {
        assertEquals(0L, remainingTransportBudgetNs(10000, 10000))
        assertEquals(0L, remainingTransportBudgetNs(10000, 10001))
        assertEquals(0L, remainingTransportBudgetNs(0, 0))
    }

    @Test
    fun `negative deadline against a later now yields zero`() {
        assertEquals(0L, remainingTransportBudgetNs(-5, 0))
    }

    @Test
    fun `positive budget for now before deadline`() {
        assertEquals(1L, remainingTransportBudgetNs(1, 0))
    }

    @Test
    fun `signed boundary wrap preserves forward elapsed`() {
        assertEquals(10L, remainingTransportBudgetNs(Long.MIN_VALUE + 4, Long.MAX_VALUE - 5))
        assertEquals(0L, remainingTransportBudgetNs(Long.MIN_VALUE, 0))
    }
}
