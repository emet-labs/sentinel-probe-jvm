package dev.emet.sentinel.probe.sdk.sagashop

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfSystemProperty

// SagaShop end-to-end integration — disabled placeholder, blocked by #22 (decision endpoint)
// and #23 (SagaShop Probe), neither of which has landed yet. Acceptance criterion 5 of issue
// #31 requires this integration; the test is declared here so it is not forgotten. It will be
// enabled and implemented when #22/#23 land.
//
// JUnit 5 `@Disabled` keeps the test out of the run entirely (no wall clock, no network), and
// the opt-in system property lets a future CI lane flip it on once the endpoint exists, without
// editing this file. Mirrors the Python SDK's xfail-marked test_sagashop_integration.py.
@Disabled("blocked on #22/#23: decision endpoint and SagaShop Probe are not yet available")
class SagaShopIntegrationTests {
    @Test
    @EnabledIfSystemProperty(named = "sentinel.sagashop.integration", matches = "true")
    fun `sagashop end to end - emit, project, enforce, verify`() {
        // End-to-end: emit a ProducerEvent from a SagaShop probe, project it through the filter,
        // enforce it through the gate against the real decision endpoint, and verify the outcome.
        // Blocked until #22 (decision endpoint) and #23 (SagaShop Probe) land.
        throw NotImplementedError("blocked on #22/#23")
    }
}
