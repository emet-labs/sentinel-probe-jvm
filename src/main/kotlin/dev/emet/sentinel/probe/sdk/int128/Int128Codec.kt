// Package int128 encodes and decodes sentinel.model.v1.Int128, the 128-bit occurrence-time
// representation. Kotlin analog of sdk/go/int128.
//
// Kotlin/Java has no native int128, so java.math.BigInteger is the canonical representation —
// the idiomatic equivalent of TypeScript's bigint and Go's math/big.Int, and what makes the
// round-trip meaningful. fromInt64 covers the common nanosecond case without allocating a
// BigInteger.
//
// Encode direction is the trap. In common.proto, `low` is fixed64 (UNSIGNED) and `high` is
// sfixed64 (SIGNED), and the value is high*2^64 + low:
//
//   - encode: low = value mod 2^64 (as unsigned), high = value >> 64 (ARITHMETIC shift);
//   - decode: value = high*2^64 + low, with NO sign correction — high is already signed.
//
// Worked check for value = -1: high = -1, low = 0xFFFFFFFFFFFFFFFF, and
// -1*2^64 + (2^64 - 1) = -1. Applying a second sign correction on decode is a real bug; it is
// not applied here either.
//
// The generated Java getters return the fixed64 `low` as a signed `long` holding the unsigned
// bit pattern. unsignedLongToBigInt widens it correctly for values above 2^63, which
// BigInteger.valueOf(long) would otherwise misread as negative.
package dev.emet.sentinel.probe.sdk.int128

import java.math.BigInteger
import java.time.Instant
import dev.emet.sentinel.model.v1.Int128

public object Int128Codec {
    // twoPow64 is 2^64, the weight of the high word.
    private val twoPow64: BigInteger = BigInteger.ONE.shiftLeft(64)

    // lowMask masks the low 64 bits. BigInteger bitwise operations behave as if operands were
    // in infinite two's complement, so And also yields the correct unsigned low word for
    // negatives.
    private val lowMask: BigInteger = twoPow64.subtract(BigInteger.ONE)

    // unsignedLongToBigInt interprets a long as an UNSIGNED 64-bit value. Negative longs hold
    // bit patterns in [2^63, 2^64), which BigInteger.valueOf would misread as negative.
    private fun unsignedLongToBigInt(value: Long): BigInteger =
        if (value >= 0) BigInteger.valueOf(value) else BigInteger.valueOf(value).add(twoPow64)

    // toBigInt decodes an Int128 into its integer value: high*2^64 + low. A null Int128 decodes
    // to zero, matching the generated getters' null-safety.
    public fun toBigInt(value: Int128?): BigInteger {
        if (value == null) return BigInteger.ZERO
        // high is sfixed64: already signed, so valueOf is correct.
        val high = BigInteger.valueOf(value.high)
        // low is fixed64 (unsigned): widen via the unsigned path.
        val low = unsignedLongToBigInt(value.low)
        return high.multiply(twoPow64).add(low)
    }

    // fromBigInt encodes an integer value into an Int128. Values outside the signed 128-bit
    // range are truncated to their low 128 bits, the same modular behaviour the proto's fixed
    // words have.
    public fun fromBigInt(value: BigInteger): Int128 {
        val low = value.and(lowMask)
        // shiftRight is an arithmetic (floor) shift on negatives: sign-extends, matching Go's
        // big.Int.Rsh.
        val high = value.shiftRight(64)
        return Int128.newBuilder()
            .setHigh(high.toLong())
            .setLow(low.toLong())
            .build()
    }

    // fromInt64 encodes an int64 without allocating a BigInteger. high is the sign extension
    // (0 or -1) and low is the two's-complement bit pattern. Kotlin `shr` is arithmetic.
    public fun fromInt64(value: Long): Int128 =
        Int128.newBuilder()
            .setHigh(value shr 63)
            .setLow(value)
            .build()

    // timeToNanoseconds returns an instant as nanoseconds since the Unix epoch. Analog of
    // Go's TimeToNanoseconds and TypeScript's hrTimeToNanoseconds.
    //
    // Deliberately not Instant.toEpochMilli() * 1e6 (millisecond precision loss) nor a single
    // long (undefined outside the long range): the BigInteger form is exact for every
    // representable instant and is the literal analog of the reference's seconds*1e9 + nanos.
    public fun timeToNanoseconds(instant: Instant): BigInteger {
        val seconds = BigInteger.valueOf(instant.epochSecond)
        val nanos = BigInteger.valueOf(instant.nano.toLong())
        return seconds.multiply(BigInteger.valueOf(1_000_000_000L)).add(nanos)
    }
}
