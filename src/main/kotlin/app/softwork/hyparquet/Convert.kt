package app.softwork.hyparquet

import java.util.Date

/**
 * Default type parsers when no custom ones are given
 */
val DEFAULT_PARSERS: ParquetParsers = object : ParquetParsers {
    override fun timestampFromMilliseconds(millis: Long): Any {
        return Date(millis)
    }

    override fun timestampFromMicroseconds(micros: Long): Any {
        return Date(micros / 1000)
    }

    override fun timestampFromNanoseconds(nanos: Long): Any {
        return Date(nanos / 1000000)
    }

    override fun dateFromDays(days: Int): Any {
        return Date(days * 86400000L)
    }

    override fun stringFromBytes(bytes: ByteArray): String {
        return bytes.decodeToString()
    }
}

/**
 * Convert known types from primitive to rich, and dereference dictionary.
 *
 * @param data series of primitive types
 * @param dictionary
 * @param encoding
 * @param columnDecoder
 * @returns series of rich types
 */
fun convertWithDictionary(
    data: DecodedArray,
    dictionary: DecodedArray?,
    encoding: Encoding,
    columnDecoder: ColumnDecoder
): DecodedArray {
    if (dictionary != null && (encoding == Encoding.PLAIN_DICTIONARY || encoding == Encoding.RLE_DICTIONARY)) {
        // Dereference dictionary
        return when (data) {
            is DecodedArray.IntArrayType -> {
                val dictList = when (dictionary) {
                    is DecodedArray.AnyArrayType -> dictionary.array
                    else -> error("Unsupported dictionary type")
                }
                DecodedArray.AnyArrayType(data.array.map { dictList[it] })
            }
            is DecodedArray.ByteArrayType -> {
                val dictList = when (dictionary) {
                    is DecodedArray.AnyArrayType -> dictionary.array
                    else -> error("Unsupported dictionary type")
                }
                DecodedArray.AnyArrayType(data.array.map { dictList[it.toInt() and 0xFF] })
            }
            else -> convert(data, columnDecoder)
        }
    } else {
        return convert(data, columnDecoder)
    }
}

/**
 * Convert known types from primitive to rich.
 *
 * @param data series of primitive types
 * @param columnDecoder - uses element, utf8, and parsers properties
 * @returns series of rich types
 */
fun convert(data: DecodedArray, columnDecoder: ColumnDecoder): DecodedArray {
    val element = columnDecoder.element
    val parsers = columnDecoder.parsers ?: DEFAULT_PARSERS
    val utf8 = columnDecoder.utf8 ?: true
    val type = element.type
    val ctype = element.converted_type
    val ltype = element.logical_type

    if (ctype == ConvertedType.DECIMAL) {
        val scale = element.scale ?: 0
        val factor = Math.pow(10.0, -scale.toDouble())
        return when (data) {
            is DecodedArray.AnyArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { value ->
                    when (value) {
                        is ByteArray -> parseDecimal(value) * factor
                        is Number -> value.toDouble() * factor
                        else -> value
                    }
                })
            }
            is DecodedArray.IntArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { it * factor })
            }
            is DecodedArray.LongArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { it * factor })
            }
            else -> data
        }
    }
    
    if (ctype == null && type == ParquetType.INT96) {
        return when (data) {
            is DecodedArray.AnyArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { value ->
                    when (value) {
                        is Long -> parsers.timestampFromNanoseconds(parseInt96Nanos(value))
                        else -> value
                    }
                })
            }
            else -> data
        }
    }

    if (ctype == ConvertedType.DATE) {
        return when (data) {
            is DecodedArray.IntArrayType -> DecodedArray.AnyArrayType(data.array.map { parsers.dateFromDays(it) })
            is DecodedArray.AnyArrayType -> DecodedArray.AnyArrayType(data.array.map { 
                when (it) {
                    is Int -> parsers.dateFromDays(it)
                    else -> it
                }
            })
            else -> data
        }
    }

    if (ctype == ConvertedType.TIMESTAMP_MILLIS) {
        return when (data) {
            is DecodedArray.LongArrayType -> DecodedArray.AnyArrayType(data.array.map { parsers.timestampFromMilliseconds(it) })
            else -> data
        }
    }

    if (ctype == ConvertedType.TIMESTAMP_MICROS) {
        return when (data) {
            is DecodedArray.LongArrayType -> DecodedArray.AnyArrayType(data.array.map { parsers.timestampFromMicroseconds(it) })
            else -> data
        }
    }

    if (ctype == ConvertedType.JSON) {
        return when (data) {
            is DecodedArray.AnyArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { value ->
                    when (value) {
                        is ByteArray -> kotlinx.serialization.json.Json.parseToJsonElement(value.decodeToString())
                        else -> value
                    }
                })
            }
            else -> data
        }
    }

    if (ctype == ConvertedType.BSON) {
        throw Error("parquet bson not supported")
    }

    if (ctype == ConvertedType.INTERVAL) {
        throw Error("parquet interval not supported")
    }

    if (ctype == ConvertedType.UTF8 || (ltype is LogicalType.STRING) || (utf8 && type == ParquetType.BYTE_ARRAY)) {
        return when (data) {
            is DecodedArray.AnyArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { value ->
                    when (value) {
                        is ByteArray -> parsers.stringFromBytes(value)
                        else -> value
                    }
                })
            }
            else -> data
        }
    }

    if (ctype == ConvertedType.UINT_64 || (ltype is LogicalType.INTEGER && ltype.bitWidth == 64 && !ltype.isSigned)) {
        return when (data) {
            is DecodedArray.LongArrayType -> data // Keep as Long, Kotlin doesn't have unsigned arrays in same way
            else -> data
        }
    }

    if (ctype == ConvertedType.UINT_32 || (ltype is LogicalType.INTEGER && ltype.bitWidth == 32 && !ltype.isSigned)) {
        return data // Keep as is
    }

    if (ltype is LogicalType.FLOAT16) {
        return when (data) {
            is DecodedArray.AnyArrayType -> {
                DecodedArray.AnyArrayType(data.array.map { value ->
                    when (value) {
                        is ByteArray -> parseFloat16(value)
                        else -> value
                    }
                })
            }
            else -> data
        }
    }

    if (ltype is LogicalType.TIMESTAMP) {
        val parser = when (ltype.unit) {
            TimeUnit.MILLIS -> parsers::timestampFromMilliseconds
            TimeUnit.MICROS -> parsers::timestampFromMicroseconds
            TimeUnit.NANOS -> parsers::timestampFromNanoseconds
        }
        return when (data) {
            is DecodedArray.LongArrayType -> DecodedArray.AnyArrayType(data.array.map { parser(it) })
            else -> data
        }
    }

    return data
}

/**
 * @param bytes
 * @returns
 */
fun parseDecimal(bytes: ByteArray): Double {
    if (bytes.isEmpty()) return 0.0

    var value = 0L
    for (byte in bytes) {
        value = value * 256 + (byte.toLong() and 0xFF)
    }

    // handle signed
    val bits = bytes.size * 8
    if (value >= (1L shl (bits - 1))) {
        value -= (1L shl bits)
    }

    return value.toDouble()
}

/**
 * Converts INT96 date format (hi 32bit days, lo 64bit nanos) to nanos since epoch
 * @param value
 * @returns
 */
fun parseInt96Nanos(value: Long): Long {
    val days = (value shr 64) - 2440588L
    val nano = value and -1L // all bits set = 0xffffffffffffffffL
    return days * 86400000000000L + nano
}

/**
 * @param bytes
 * @returns
 */
fun parseFloat16(bytes: ByteArray?): Double? {
    if (bytes == null) return null
    val int16 = ((bytes[1].toInt() and 0xFF) shl 8) or (bytes[0].toInt() and 0xFF)
    val sign = if ((int16 shr 15) != 0) -1.0 else 1.0
    val exp = (int16 shr 10) and 0x1f
    val frac = int16 and 0x3ff
    if (exp == 0) return sign * Math.pow(2.0, -14.0) * (frac / 1024.0) // subnormals
    if (exp == 0x1f) return if (frac != 0) Double.NaN else sign * Double.POSITIVE_INFINITY
    return sign * Math.pow(2.0, (exp - 15).toDouble()) * (1 + frac / 1024.0)
}
