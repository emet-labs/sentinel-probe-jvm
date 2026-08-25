// Package config reads a Probe's source tier from deployment configuration (ADR-0022).
// Kotlin analog of sdk/go/config/sourcetier.go, which collapses schema.ts and source-tier.ts
// into one file (divergence D4): without zod the "schema" is a map plus a validation function,
// and two files would be ceremony.
//
// Tier is NEVER hard-coded. There is deliberately no list of anchor handles anywhere in this
// SDK: the deployment declares tiers, the Probe reads them at init and looks up its own
// source_handle. An undeclared handle is an error, never a silent default — defaulting would
// quietly demote or promote a source's evidentiary weight.
package dev.emet.sentinel.probe.sdk.config

import dev.emet.sentinel.model.v1.SourceTier

public object SourceTierConfig {
    // Tier values accepted in configuration. These are the deployment-facing spellings, not the
    // proto constant names; tierForHandle maps them to SourceTier.
    public const val TIER_ANCHOR: String = "ANCHOR"
    public const val TIER_CONTRIBUTING: String = "CONTRIBUTING"
}

// SourceEntry is one source's declared configuration.
public data class SourceEntry(
    // tier is "ANCHOR" or "CONTRIBUTING". Any other value is rejected at parse time.
    val tier: String,
    // extra holds every other key in the entry, preserved rather than rejected. This mirrors
    // the reference's zod .passthrough(): a deployment may carry its own annotations alongside
    // the tier, and the SDK is not the right place to police them.
    val extra: Map<String, Any?>,
)

// SourceTierMap maps source_handle to its declared entry. A typealias over Map keeps the
// parser-agnostic shape the reference's loadSourceTierConfig takes `unknown` for.
public typealias SourceTierMap = Map<String, SourceEntry>

public object SourceTiers {
    // loadSourceTierConfig parses JSON configuration text. Throws IllegalArgumentException on
    // invalid JSON, a non-object document, or an entry with a missing/non-string/unknown tier.
    public fun load(raw: String): SourceTierMap {
        val decoded = parseJsonObject(raw)
        return parse(decoded)
    }

    // parse validates configuration that has already been decoded, so a host can use whatever
    // parser it likes — the reference's loadSourceTierConfig takes `unknown` for the same
    // reason. YAML decoding in particular stays out of the SDK: the host decodes and calls this.
    public fun parse(raw: Map<String, Any?>): SourceTierMap {
        val config = LinkedHashMap<String, SourceEntry>(raw.size)
        for ((handle, value) in raw) {
            val entry =
                value as? Map<*, *>
                    ?: throw IllegalArgumentException(
                        "source-tier: entry for \"$handle\" must be an object, got ${value?.javaClass?.name ?: "null"}",
                    )

            @Suppress("UNCHECKED_CAST")
            val entryMap = entry as Map<String, Any?>
            val tierValue =
                entryMap["tier"]
                    ?: throw IllegalArgumentException("source-tier: entry for \"$handle\" has no tier")
            val tier =
                tierValue as? String
                    ?: throw IllegalArgumentException(
                        "source-tier: tier for \"$handle\" must be a string, got ${tierValue.javaClass.name}",
                    )
            if (tier != SourceTierConfig.TIER_ANCHOR && tier != SourceTierConfig.TIER_CONTRIBUTING) {
                throw IllegalArgumentException(
                    "source-tier: unknown tier \"$tier\" for \"$handle\", want " +
                        "\"${SourceTierConfig.TIER_ANCHOR}\" or \"${SourceTierConfig.TIER_CONTRIBUTING}\"",
                )
            }
            val extra = LinkedHashMap<String, Any?>(entryMap.size - 1)
            for ((key, extraValue) in entryMap) {
                if (key != "tier") extra[key] = extraValue
            }
            config[handle] = SourceEntry(tier = tier, extra = extra)
        }
        return config
    }

    // tierForHandle resolves the proto SourceTier for a source_handle.
    //
    // An undeclared handle is an error. It is never SOURCE_TIER_UNSPECIFIED and never a default:
    // a source whose tier nobody declared has no business claiming one.
    public fun tierForHandle(
        config: SourceTierMap,
        sourceHandle: String,
    ): SourceTier {
        val entry =
            config[sourceHandle]
                ?: throw IllegalArgumentException(
                    "source-tier: no entry for source_handle \"$sourceHandle\"",
                )
        return when (entry.tier) {
            SourceTierConfig.TIER_ANCHOR -> SourceTier.SOURCE_TIER_ANCHOR
            SourceTierConfig.TIER_CONTRIBUTING -> SourceTier.SOURCE_TIER_CONTRIBUTING
            else -> throw IllegalArgumentException(
                "source-tier: unknown tier \"${entry.tier}\" for source_handle \"$sourceHandle\"",
            )
        }
    }

    // parseJsonObject is a minimal JSON object parser. The SDK deliberately avoids pulling a JSON
    // library onto the runtime classpath for a configuration helper; the JDK has no JSON parser
    // either, so a tiny recursive-descent parser covers the structured config the reference uses
    // (objects of objects with string tiers and arbitrary extra values). It accepts the JSON
    // subset zod .passthrough() operates on: objects, arrays, strings, numbers, booleans, null.
    private fun parseJsonObject(text: String): Map<String, Any?> {
        val parser = JsonParser(text)
        val value = parser.parseValue()
        parser.skipWhitespace()
        parser.expectEof()
        @Suppress("UNCHECKED_CAST")
        return value as? Map<String, Any?>
            ?: throw IllegalArgumentException("source-tier: config must be an object")
    }
}
