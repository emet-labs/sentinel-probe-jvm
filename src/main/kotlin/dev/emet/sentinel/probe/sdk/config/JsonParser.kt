// A minimal JSON parser used by SourceTiers.load to read deployment configuration without
// pulling a JSON library onto the runtime classpath. The JDK ships no JSON parser, and the
// reference's zod .passthrough() operates on a small JSON subset (objects of objects with
// string tiers and arbitrary extra values), so a tiny recursive-descent parser covers it.
//
// This is NOT a general-purpose JSON library; it throws IllegalArgumentException on malformed
// input so configuration errors surface at load time, exactly as the Go reference's
// encoding/json does.
package dev.emet.sentinel.probe.sdk.config

internal class JsonParser(
    private val source: String,
) {
    private var index: Int = 0

    fun parseValue(): Any? {
        skipWhitespace()
        require(index < source.length) { "source-tier: unexpected end of JSON" }
        return when (val c = source[index]) {
            '{' -> parseObject()
            '[' -> parseArray()
            '"' -> parseString()
            't', 'f' -> parseBoolean()
            'n' -> parseNull()
            else -> parseNumberOrFail(c)
        }
    }

    fun expectEof() {
        require(index >= source.length || source[index].isWhitespace()) {
            "source-tier: trailing characters in JSON at index $index"
        }
    }

    fun skipWhitespace() {
        while (index < source.length && source[index].isWhitespace()) index++
    }

    private fun parseObject(): Map<String, Any?> {
        expect('{')
        val map = LinkedHashMap<String, Any?>()
        skipWhitespace()
        if (peek() == '}') {
            index++
            return map
        }
        while (true) {
            skipWhitespace()
            val key = parseString()
            skipWhitespace()
            expect(':')
            val value = parseValue()
            map[key] = value
            skipWhitespace()
            when (peek()) {
                ',' -> {
                    index++
                }
                '}' -> {
                    index++
                    return map
                }
                else -> throw IllegalArgumentException("source-tier: expected ',' or '}' in object at index $index")
            }
        }
    }

    private fun parseArray(): List<Any?> {
        expect('[')
        val list = ArrayList<Any?>()
        skipWhitespace()
        if (peek() == ']') {
            index++
            return list
        }
        while (true) {
            list.add(parseValue())
            skipWhitespace()
            when (peek()) {
                ',' -> {
                    index++
                }
                ']' -> {
                    index++
                    return list
                }
                else -> throw IllegalArgumentException("source-tier: expected ',' or ']' in array at index $index")
            }
        }
    }

    private fun parseString(): String {
        expect('"')
        val sb = StringBuilder()
        while (index < source.length) {
            val c = source[index++]
            when {
                c == '"' -> return sb.toString()
                c == '\\' -> {
                    require(index < source.length) { "source-tier: unterminated escape in string" }
                    val esc = source[index++]
                    sb.append(
                        when (esc) {
                            '"' -> '"'
                            '\\' -> '\\'
                            '/' -> '/'
                            'b' -> '\b'
                            'f' -> '\u000C'
                            'n' -> '\n'
                            'r' -> '\r'
                            't' -> '\t'
                            'u' -> {
                                val hex = source.substring(index, index + 4)
                                index += 4
                                hex.toInt(16).toChar()
                            }
                            else -> throw IllegalArgumentException("source-tier: invalid escape \\$esc in string")
                        },
                    )
                }
                else -> sb.append(c)
            }
        }
        throw IllegalArgumentException("source-tier: unterminated string")
    }

    private fun parseBoolean(): Boolean {
        if (source.startsWith("true", index)) {
            index += 4
            return true
        }
        if (source.startsWith("false", index)) {
            index += 5
            return false
        }
        throw IllegalArgumentException("source-tier: invalid literal at index $index")
    }

    private fun parseNull(): Any? {
        if (source.startsWith("null", index)) {
            index += 4
            return null
        }
        throw IllegalArgumentException("source-tier: invalid literal at index $index")
    }

    private fun parseNumberOrFail(first: Char): Any {
        // A number is anything matching -?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?. Keep it as a
        // Double for uniformity; SourceTiers only inspects string tiers and opaque extra values.
        require(first == '-' || first in '0'..'9') {
            "source-tier: unexpected character '$first' at index $index"
        }
        val start = index
        if (peek() == '-') index++
        while (index < source.length && source[index].isDigit()) index++
        var isDouble = false
        if (peek() == '.') {
            isDouble = true
            index++
            while (index < source.length && source[index].isDigit()) index++
        }
        if (index < source.length && (source[index] == 'e' || source[index] == 'E')) {
            isDouble = true
            index++
            if (index < source.length && (source[index] == '+' || source[index] == '-')) index++
            while (index < source.length && source[index].isDigit()) index++
        }
        val token = source.substring(start, index)
        return if (isDouble) token.toDouble() else token.toLong()
    }

    private fun peek(): Char {
        require(index < source.length) { "source-tier: unexpected end of JSON" }
        return source[index]
    }

    private fun expect(c: Char) {
        require(index < source.length && source[index] == c) {
            "source-tier: expected '$c' at index $index"
        }
        index++
    }
}
