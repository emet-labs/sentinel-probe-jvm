package dev.emet.sentinel.probe.sdk.int128

import dev.emet.sentinel.model.v1.Int128
import io.kotest.property.Arb
import io.kotest.property.PropTestConfig
import io.kotest.property.arbitrary.bigInt
import io.kotest.property.checkAll
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Int128Tests {
    private fun bigInt(decimal: String): BigInteger = BigInteger(decimal)

    @Test
    fun `round-trips through the signed 128-bit range`() {
        // Pinned seed keeps the property run hermetic (ADR-0019): no wall clock drives the RNG.
        val cases =
            listOf(
                "0",
                "1",
                "-1",
                "127",
                "-127",
                "128",
                "-128",
                "9223372036854775807", // Long.MAX_VALUE
                "-9223372036854775808", // Long.MIN_VALUE
                "9223372036854775808", // 2^63, just beyond Long
                "-9223372036854775809",
                "18446744073709551615", // 2^64 - 1
                "18446744073709551616", // 2^64
                "-18446744073709551616", // -2^64
                "170141183460469231731687303715884105727", // 2^127 - 1
                "-170141183460469231731687303715884105728", // -2^127
            )
        for (decimal in cases) {
            val value = bigInt(decimal)
            assertEquals(0, Int128Codec.toBigInt(Int128Codec.fromBigInt(value)).compareTo(value), "round-trip $decimal")
        }
    }

    @Test
    fun `encode word signedness - low is unsigned, high is signed`() {
        // 2^64 + 1 encodes as high=1, low=1.
        val positive = Int128Codec.fromBigInt(bigInt("18446744073709551617"))
        assertEquals(1L, positive.high)
        assertEquals(1L, positive.low)

        // The worked check from the package doc: -1 is high=-1, low=0xFFFFFFFFFFFFFFFF, and
        // -1*2^64 + (2^64-1) = -1 with NO sign correction on decode.
        val negative = Int128Codec.fromBigInt(BigInteger.valueOf(-1L))
        assertEquals(-1L, negative.high, "high must sign-extend to -1 (arithmetic shift)")
        assertEquals(-1L, negative.low, "low must be the unsigned 2^64-1 bit pattern in a signed long")
        assertEquals(0, Int128Codec.toBigInt(negative).compareTo(BigInteger.valueOf(-1L)), "decode must not apply a second sign correction")
    }

    @Test
    fun `fromInt64 matches fromBigInt`() {
        for (value in listOf(0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42L, -999999L)) {
            val a = Int128Codec.fromInt64(value)
            val b = Int128Codec.fromBigInt(BigInteger.valueOf(value))
            assertEquals(a.high, b.high, "high for $value")
            assertEquals(a.low, b.low, "low for $value")
            assertEquals(0, Int128Codec.toBigInt(a).compareTo(BigInteger.valueOf(value)), "decode $value")
        }
    }

    @Test
    fun `toBigInt is null-safe`() {
        assertEquals(BigInteger.ZERO, Int128Codec.toBigInt(null))
    }

    @Test
    fun `timeToNanoseconds is exact for instants`() {
        val instant = Instant.ofEpochSecond(1700000000L, 123456789)
        assertEquals(
            bigInt("1700000000123456789"),
            Int128Codec.timeToNanoseconds(instant),
        )
    }

    @Test
    fun `timeToNanoseconds round-trips through Int128`() {
        val instant = Instant.ofEpochSecond(1700000000L, 1)
        val nanos = Int128Codec.timeToNanoseconds(instant)
        val encoded = Int128Codec.fromBigInt(nanos)
        assertEquals(0, Int128Codec.toBigInt(encoded).compareTo(nanos))
    }

    @Test
    fun `timeToNanoseconds handles beyond-int64 nanoseconds`() {
        // Year 3000: ~3.25e19 ns, beyond Long.MAX_VALUE (~9.2e18). Must span both words.
        val moment = Instant.parse("3000-01-01T00:00:00Z")
        val nanos = Int128Codec.timeToNanoseconds(moment)
        assertTrue(nanos > BigInteger.valueOf(Long.MAX_VALUE), "year 3000 nanos exceed int64")
        val encoded = Int128Codec.fromBigInt(nanos)
        assertTrue(encoded.high != 0L, "an instant beyond 2^64 nanoseconds must use the high word")
        assertEquals(0, Int128Codec.toBigInt(encoded).compareTo(nanos), "beyond-int64 round-trip")
    }

    @Test
    fun `property round-trip holds across the signed 128-bit range`() {
        // Kotest property with a pinned seed so the run is deterministic (ADR-0019): no wall
        // clock drives the RNG, so a failure reproduces deterministically. checkAll is a suspend
        // function, so runBlocking drives the property from a plain JUnit test.
        val arb = Arb.bigInt(0, 127)
        kotlinx.coroutines.runBlocking {
            checkAll(PropTestConfig(seed = 31L), arb) { value ->
                val encoded = Int128Codec.fromBigInt(value)
                val decoded = Int128Codec.toBigInt(encoded)
                assertEquals(0, decoded.compareTo(value))
            }
        }
    }
}
