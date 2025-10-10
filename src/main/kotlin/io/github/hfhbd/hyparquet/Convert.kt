package io.github.hfhbd.hyparquet

import kotlinx.serialization.json.Json
import java.util.*
import kotlin.math.pow

/**
 * Default type parsers when no custom ones are given
 */
object DefaultParsers : ParquetParsers {
    override fun timestampFromMilliseconds(millis: Long): Date {
        return Date(millis)
    }

    override fun timestampFromMicroseconds(micros: Long): Date {
        return Date(micros / 1000)
    }

    override fun timestampFromNanoseconds(nanos: Long): Date {
        return Date(nanos / 1_000_000)
    }

    override fun dateFromDays(days: Int): Date {
        return Date(days * 86400000L)
    }

    override fun stringFromBytes(bytes: ByteArray): String {
        return String(bytes, Charsets.UTF_8)
    }
}

/**
 * ColumnDecoder interface for convert functions
 */
interface ColumnDecoder {
    val element: SchemaElement
    val utf8: Boolean
    val parsers: ParquetParsers
}

/**
 * Convert known types from primitive to rich, and dereference dictionary.
 */
fun convertWithDictionary(
    data: List<Any>,
    dictionary: List<Any>?,
    encoding: Encoding,
    columnDecoder: ColumnDecoder
): List<Any> {
    return if (dictionary != null && encoding.name.endsWith("_DICTIONARY")) {
        data.map { index ->
            when (index) {
                is Int -> dictionary.getOrNull(index)
                is Long -> dictionary.getOrNull(index.toInt())
                else -> dictionary.getOrNull(index.toString().toIntOrNull() ?: 0)
            } ?: index
        }
    } else {
        convert(data, columnDecoder)
    }
}

/**
 * Convert known types from primitive to rich.
 */
fun convert(data: List<Any>, columnDecoder: ColumnDecoder): List<Any> {
    val element = columnDecoder.element
    val parsers = columnDecoder.parsers
    val utf8 = columnDecoder.utf8
    
    val type = element.type
    val convertedType = element.convertedType
    val logicalType = element.logicalType

    when (convertedType) {
        ConvertedType.DECIMAL -> {
            val scale = element.scale ?: 0
            val factor = 10.0.pow(-scale)
            return data.map { value ->
                when (value) {
                    is ByteArray -> parseDecimal(value) * factor
                    else -> (value as? Number)?.toDouble()?.times(factor) ?: value
                }
            }
        }
        ConvertedType.DATE -> {
            return data.map { value ->
                parsers.dateFromDays((value as Number).toInt())
            }
        }
        ConvertedType.TIMESTAMP_MILLIS -> {
            return data.map { value ->
                parsers.timestampFromMilliseconds((value as Number).toLong())
            }
        }
        ConvertedType.TIMESTAMP_MICROS -> {
            return data.map { value ->
                parsers.timestampFromMicroseconds((value as Number).toLong())
            }
        }
        ConvertedType.JSON -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> Json.parseToJsonElement(String(value, Charsets.UTF_8))
                    is String -> Json.parseToJsonElement(value)
                    else -> value
                }
            }
        }
        ConvertedType.BSON -> {
            throw UnsupportedOperationException("BSON not supported")
        }
        ConvertedType.INTERVAL -> {
            throw UnsupportedOperationException("INTERVAL not supported")
        }
        ConvertedType.UTF8 -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parsers.stringFromBytes(value)
                    else -> value
                }
            }
        }
        ConvertedType.UINT_64 -> {
            return data.map { value ->
                when (value) {
                    is Long -> if (value < 0) value + (1L shl 63) else value
                    else -> (value as Number).toLong()
                }
            }
        }
        ConvertedType.UINT_32 -> {
            return data.map { value ->
                when (value) {
                    is Int -> if (value < 0) value + (1L shl 32) else value.toLong()
                    else -> (value as Number).toLong()
                }
            }
        }
        else -> {
            // Handle cases based on type and logical type
            when {
                type == ParquetType.INT96 && convertedType == null -> {
                    return data.map { value ->
                        parsers.timestampFromNanoseconds(parseInt96Nanos(value as Long))
                    }
                }
                logicalType is LogicalType.STRING || 
                (utf8 && type == ParquetType.BYTE_ARRAY) -> {
                    return data.map { value ->
                        when (value) {
                            is ByteArray -> parsers.stringFromBytes(value)
                            else -> value
                        }
                    }
                }
                logicalType is LogicalType.INTEGER -> {
                    if (logicalType.bitWidth == 64 && !logicalType.isSigned) {
                        return data.map { value ->
                            val longValue = (value as Number).toLong()
                            if (longValue < 0) longValue + (1L shl 63) else longValue
                        }
                    } else if (logicalType.bitWidth == 32 && !logicalType.isSigned) {
                        return data.map { value ->
                            val intValue = (value as Number).toInt()
                            if (intValue < 0) intValue + (1L shl 32) else intValue.toLong()
                        }
                    }
                }
                logicalType is LogicalType.FLOAT16 -> {
                    return data.map { value ->
                        when (value) {
                            is ByteArray -> parseFloat16(value)
                            else -> value
                        }
                    }
                }
                logicalType is LogicalType.TIMESTAMP -> {
                    val parser = when (logicalType.unit) {
                        TimeUnit.MICROS -> parsers::timestampFromMicroseconds
                        TimeUnit.NANOS -> parsers::timestampFromNanoseconds
                        else -> parsers::timestampFromMilliseconds
                    }
                    return data.map { value ->
                        parser((value as Number).toLong())
                    }
                }
            }
        }
    }

    return data
}

/**
 * Parse decimal from byte array
 */
fun parseDecimal(bytes: ByteArray): Double {
    if (bytes.isEmpty()) return 0.0

    var value = 0L
    for (byte in bytes) {
        value = value * 256 + (byte.toInt() and 0xFF)
    }

    // Handle signed values
    val bits = bytes.size * 8
    if (bits < 64) {
        val signMask = 1L shl (bits - 1)
        if (value >= signMask) {
            value -= (1L shl bits)
        }
    }

    return value.toDouble()
}

/**
 * Converts INT96 date format (hi 32bit days, lo 64bit nanos) to nanos since epoch
 */
private fun parseInt96Nanos(value: Long): Long {
    val days = (value ushr 32) - 2440588L
    val nanos = value and 0xFFFFFFFFL
    return days * 86400000000000L + nanos
}

/**
 * Parse 16-bit floating point number from byte array
 */
fun parseFloat16(bytes: ByteArray?): Float? {
    if (bytes == null || bytes.size < 2) return null
    
    val int16 = (bytes[1].toInt() and 0xFF shl 8) or (bytes[0].toInt() and 0xFF)
    val sign = if ((int16 ushr 15) != 0) -1f else 1f
    val exp = (int16 ushr 10) and 0x1F
    val frac = int16 and 0x3FF

    return when {
        exp == 0 -> sign * 2.0f.pow(-14) * (frac / 1024f) // subnormals
        exp == 0x1F -> if (frac != 0) Float.NaN else sign * Float.POSITIVE_INFINITY
        else -> sign * 2.0f.pow(exp - 15) * (1 + frac / 1024f)
    }
}