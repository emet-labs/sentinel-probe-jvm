package dev.emet.sentinel.probe.sdk.config

import dev.emet.sentinel.model.v1.SourceTier
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SourceTierTests {
    private fun mustLoad(document: String): SourceTierMap = SourceTiers.load(document)

    @Test
    fun `valid config parses`() {
        val config = mustLoad(
            """
            {
              "gateway.tool-calls": { "tier": "ANCHOR" },
              "ledger.writer": { "tier": "CONTRIBUTING" }
            }
            """,
        )
        assertEquals(SourceTierConfig.TIER_ANCHOR, config["gateway.tool-calls"]?.tier)
        assertEquals(SourceTierConfig.TIER_CONTRIBUTING, config["ledger.writer"]?.tier)
    }

    @Test
    fun `tierForHandle resolves both tiers`() {
        val config = mustLoad(
            """
            {
              "anchor.src": { "tier": "ANCHOR" },
              "contrib.src": { "tier": "CONTRIBUTING" }
            }
            """,
        )
        assertEquals(SourceTier.SOURCE_TIER_ANCHOR, SourceTiers.tierForHandle(config, "anchor.src"))
        assertEquals(SourceTier.SOURCE_TIER_CONTRIBUTING, SourceTiers.tierForHandle(config, "contrib.src"))
    }

    @Test
    fun `undeclared handle is an error, never a default`() {
        val config = mustLoad("""{ "anchor.src": { "tier": "ANCHOR" } }""")
        val ex = assertThrows<IllegalArgumentException> {
            SourceTiers.tierForHandle(config, "nobody.declared.me")
        }
        assertTrue(ex.message!!.contains("no entry for source_handle"))
    }

    @Test
    fun `empty config never defaults`() {
        val config = SourceTiers.load("{}")
        assertThrows<IllegalArgumentException> { SourceTiers.tierForHandle(config, "any.handle") }
    }

    @Test
    fun `invalid tier value is rejected`() {
        val ex = assertThrows<IllegalArgumentException> {
            mustLoad("""{ "src": { "tier": "UNKNOWN" } }""")
        }
        assertTrue(ex.message!!.contains("unknown tier \"UNKNOWN\""))
    }

    @Test
    fun `non-string tier is rejected`() {
        val ex = assertThrows<IllegalArgumentException> {
            mustLoad("""{ "src": { "tier": 7 } }""")
        }
        assertTrue(ex.message!!.contains("must be a string"))
    }

    @Test
    fun `missing tier is rejected`() {
        val ex = assertThrows<IllegalArgumentException> {
            mustLoad("""{ "src": { "note": "no tier here" } }""")
        }
        assertTrue(ex.message!!.contains("has no tier"))
    }

    @Test
    fun `non-object config is rejected`() {
        val ex = assertThrows<IllegalArgumentException> {
            SourceTiers.load("""[1, 2, 3]""")
        }
        assertTrue(ex.message!!.contains("must be an object"))
    }

    @Test
    fun `non-object entry is rejected`() {
        val ex = assertThrows<IllegalArgumentException> {
            mustLoad("""{ "src": "not an object" }""")
        }
        assertTrue(ex.message!!.contains("must be an object"))
    }

    @Test
    fun `extra keys are preserved (passthrough)`() {
        val config = mustLoad(
            """{ "src": { "tier": "ANCHOR", "owner": "team-a", "deployed_at": "2024-01" } }""",
        )
        val entry = config["src"]!!
        assertEquals("ANCHOR", entry.tier)
        assertEquals("team-a", entry.extra["owner"])
        assertEquals("2024-01", entry.extra["deployed_at"])
    }

    @Test
    fun `dotted handles are valid keys`() {
        val config = mustLoad("""{ "gateway.tool-calls": { "tier": "ANCHOR" } }""")
        assertEquals(SourceTier.SOURCE_TIER_ANCHOR, SourceTiers.tierForHandle(config, "gateway.tool-calls"))
    }

    @Test
    fun `parse is parser-agnostic - accepts a pre-decoded map`() {
        val decoded = mapOf("src" to mapOf("tier" to "CONTRIBUTING"))
        val config = SourceTiers.parse(decoded)
        assertEquals(SourceTier.SOURCE_TIER_CONTRIBUTING, SourceTiers.tierForHandle(config, "src"))
    }

    @Test
    fun `tierForHandle rejects a hand-built unknown tier`() {
        // A caller can build a SourceTierMap literal with an unknown tier; the lookup must still
        // throw rather than silently map to UNSPECIFIED.
        val config: SourceTierMap = mapOf("src" to SourceEntry(tier = "BOGUS", extra = emptyMap()))
        val ex = assertThrows<IllegalArgumentException> {
            SourceTiers.tierForHandle(config, "src")
        }
        assertTrue(ex.message!!.contains("unknown tier \"BOGUS\""))
    }

    @Test
    fun `invalid JSON is rejected`() {
        val ex = assertThrows<IllegalArgumentException> { SourceTiers.load("{ not json") }
        assertTrue(ex.message!!.startsWith("source-tier:"))
    }
}
