package app.hyperparam.hyparquet

import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

/**
 * Data reader interface for WKB parsing
 */
data class DataReader(
    val view: ByteBuffer,
    var offset: Int = 0
)

/**
 * Geometry object representing GeoJSON-like structure
 */
data class Geometry(
    val type: String,
    val coordinates: Any?,
    val geometries: List<Geometry>? = null
)

/**
 * Parsers for different data types in Parquet files
 */
interface ParquetParsers {
    fun timestampFromMilliseconds(millis: BigInteger): Any
    fun timestampFromMicroseconds(micros: BigInteger): Any  
    fun timestampFromNanoseconds(nanos: BigInteger): Any
    fun dateFromDays(days: Int): Any
    fun stringFromBytes(bytes: ByteArray?): String?
    fun geometryFromBytes(bytes: ByteArray?): Geometry?
    fun geographyFromBytes(bytes: ByteArray?): Geometry?
}

/**
 * Default type parsers when no custom ones are given
 */
object DefaultParsers : ParquetParsers {
    override fun timestampFromMilliseconds(millis: BigInteger): Instant {
        return Instant.ofEpochMilli(millis.toLong())
    }
    
    override fun timestampFromMicroseconds(micros: BigInteger): Instant {
        return Instant.ofEpochMilli((micros / BigInteger.valueOf(1000)).toLong())
    }
    
    override fun timestampFromNanoseconds(nanos: BigInteger): Instant {
        return Instant.ofEpochMilli((nanos / BigInteger.valueOf(1000000)).toLong())
    }
    
    override fun dateFromDays(days: Int): LocalDate {
        return LocalDate.ofEpochDay(days.toLong())
    }
    
    override fun stringFromBytes(bytes: ByteArray?): String? {
        return bytes?.let { String(it, Charsets.UTF_8) }
    }
    
    override fun geometryFromBytes(bytes: ByteArray?): Geometry? {
        return bytes?.let { 
            val buffer = ByteBuffer.wrap(it)
            val reader = DataReader(buffer)
            wkbToGeojson(reader)
        }
    }
    
    override fun geographyFromBytes(bytes: ByteArray?): Geometry? {
        return bytes?.let { 
            val buffer = ByteBuffer.wrap(it)
            val reader = DataReader(buffer)
            wkbToGeojson(reader)
        }
    }
}

/**
 * Schema element representing column metadata
 */
data class SchemaElement(
    val name: String,
    val type: String? = null,
    val convertedType: String? = null,
    val logicalType: LogicalType? = null,
    val scale: Int? = null
)

/**
 * Logical type information
 */
data class LogicalType(
    val type: String,
    val bitWidth: Int? = null,
    val isSigned: Boolean? = null,
    val unit: String? = null
)

/**
 * Column decoder containing parsing information
 */
data class ColumnDecoder(
    val element: SchemaElement,
    val utf8: Boolean = true,
    val parsers: ParquetParsers = DefaultParsers
)

/**
 * Convert known types from primitive to rich, and dereference dictionary.
 */
fun convertWithDictionary(
    data: List<Any>,
    dictionary: List<Any>?,
    encoding: String,
    columnDecoder: ColumnDecoder
): List<Any> {
    return if (dictionary != null && encoding.endsWith("_DICTIONARY")) {
        data.map { index ->
            val idx = when (index) {
                is Number -> index.toInt()
                else -> throw IllegalArgumentException("Dictionary index must be a number")
            }
            dictionary[idx]
        }
    } else {
        convert(data, columnDecoder)
    }
}

fun convert(data: List<Any>, columnDecoder: ColumnDecoder): List<Any> {
    val element = columnDecoder.element
    val parsers = columnDecoder.parsers
    val utf8 = columnDecoder.utf8
    val ctype = element.convertedType
    val ltype = element.logicalType
    val type = element.type

    when (ctype) {
        "DECIMAL" -> {
            val scale = element.scale ?: 0
            val factor = Math.pow(10.0, -scale.toDouble())
            return data.map { value ->
                when (value) {
                    is ByteArray -> parseDecimal(value) * factor
                    is Number -> value.toDouble() * factor
                    else -> throw IllegalArgumentException("Invalid decimal value: $value")
                }
            }
        }
        "DATE" -> {
            return data.map { value ->
                when (value) {
                    is Number -> parsers.dateFromDays(value.toInt())
                    else -> throw IllegalArgumentException("Invalid date value: $value")
                }
            }
        }
        "TIMESTAMP_MILLIS" -> {
            return data.map { value ->
                when (value) {
                    is BigInteger -> parsers.timestampFromMilliseconds(value)
                    is Number -> parsers.timestampFromMilliseconds(BigInteger.valueOf(value.toLong()))
                    else -> throw IllegalArgumentException("Invalid timestamp value: $value")
                }
            }
        }
        "TIMESTAMP_MICROS" -> {
            return data.map { value ->
                when (value) {
                    is BigInteger -> parsers.timestampFromMicroseconds(value)
                    is Number -> parsers.timestampFromMicroseconds(BigInteger.valueOf(value.toLong()))
                    else -> throw IllegalArgumentException("Invalid timestamp value: $value")
                }
            }
        }
        "JSON" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> kotlinx.serialization.json.Json.parseToJsonElement(String(value, Charsets.UTF_8))
                    is String -> kotlinx.serialization.json.Json.parseToJsonElement(value)
                    else -> throw IllegalArgumentException("Invalid JSON value: $value")
                }
            }
        }
        "BSON" -> {
            throw NotImplementedError("Parquet BSON not supported")
        }
        "INTERVAL" -> {
            throw NotImplementedError("Parquet interval not supported")
        }
        "UTF8" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parsers.stringFromBytes(value) ?: value
                    else -> value
                }
            }
        }
        "UINT_64" -> {
            return data.map { value ->
                when (value) {
                    is Number -> {
                        val longValue = value.toLong()
                        if (longValue < 0) throw IllegalArgumentException("Cannot convert negative number to ULong: $value")
                        longValue.toULong()
                    }
                    else -> throw IllegalArgumentException("Invalid uint64 value: $value")
                }
            }
        }
        "UINT_32" -> {
            return data.map { value ->
                when (value) {
                    is Number -> {
                        val longValue = value.toLong()
                        if (longValue < 0 || longValue > UInt.MAX_VALUE.toLong()) {
                            throw IllegalArgumentException("Value out of range for UInt: $value")
                        }
                        longValue.toUInt()
                    }
                    else -> throw IllegalArgumentException("Invalid uint32 value: $value")
                }
            }
        }
    }

    // Handle logical types
    when (ltype?.type) {
        "GEOMETRY" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parsers.geometryFromBytes(value) ?: value
                    else -> value
                }
            }
        }
        "GEOGRAPHY" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parsers.geographyFromBytes(value) ?: value
                    else -> value
                }
            }
        }
        "STRING" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parsers.stringFromBytes(value) ?: value
                    else -> value
                }
            }
        }
        "INTEGER" -> {
            when {
                ltype.bitWidth == 64 && ltype.isSigned == false -> {
                    return data.map { value ->
                        when (value) {
                            is Number -> {
                                val longValue = value.toLong()
                                if (longValue < 0) throw IllegalArgumentException("Cannot convert negative number to ULong: $value")
                                longValue.toULong()
                            }
                            else -> throw IllegalArgumentException("Invalid uint64 value: $value")
                        }
                    }
                }
                ltype.bitWidth == 32 && ltype.isSigned == false -> {
                    return data.map { value ->
                        when (value) {
                            is Number -> {
                                val longValue = value.toLong()
                                if (longValue < 0 || longValue > UInt.MAX_VALUE.toLong()) {
                                    throw IllegalArgumentException("Value out of range for UInt: $value")
                                }
                                longValue.toUInt()
                            }
                            else -> throw IllegalArgumentException("Invalid uint32 value: $value")
                        }
                    }
                }
            }
        }
        "FLOAT16" -> {
            return data.map { value ->
                when (value) {
                    is ByteArray -> parseFloat16(value) ?: value
                    else -> value
                }
            }
        }
        "TIMESTAMP" -> {
            val unit = ltype.unit
            return data.map { value ->
                val bigIntValue = when (value) {
                    is BigInteger -> value
                    is Number -> BigInteger.valueOf(value.toLong())
                    else -> throw IllegalArgumentException("Invalid timestamp value: $value")
                }
                when (unit) {
                    "MICROS" -> parsers.timestampFromMicroseconds(bigIntValue)
                    "NANOS" -> parsers.timestampFromNanoseconds(bigIntValue)
                    else -> parsers.timestampFromMilliseconds(bigIntValue)
                }
            }
        }
    }

    // Handle INT96 (no converted type)
    if (ctype == null && type == "INT96") {
        return data.map { value ->
            when (value) {
                is BigInteger -> parsers.timestampFromNanoseconds(parseInt96Nanos(value))
                else -> throw IllegalArgumentException("Invalid INT96 value: $value")
            }
        }
    }

    // Handle UTF8 default for BYTE_ARRAY
    if (utf8 && type == "BYTE_ARRAY" && ctype == null && ltype?.type != "GEOMETRY" && ltype?.type != "GEOGRAPHY") {
        return data.map { value ->
            when (value) {
                is ByteArray -> parsers.stringFromBytes(value) ?: value
                else -> value
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

    var value = BigInteger.ZERO
    for (byte in bytes) {
        value = value.multiply(BigInteger.valueOf(256)).add(BigInteger.valueOf((byte.toInt() and 0xFF).toLong()))
    }

    // Handle signed values
    val bits = bytes.size * 8
    val maxValue = BigInteger.valueOf(2).pow(bits - 1)
    if (value >= maxValue) {
        value = value.subtract(BigInteger.valueOf(2).pow(bits))
    }

    return value.toDouble()
}

fun parseInt96Nanos(value: BigInteger): BigInteger {
    val days = (value shr 64) - BigInteger.valueOf(2440588)
    val nano = value and BigInteger("18446744073709551615") // 0xffffffffffffffffL as BigInteger
    return days * BigInteger.valueOf(86400000000000L) + nano
}

/**
 * Parse 16-bit float from byte array
 */
fun parseFloat16(bytes: ByteArray?): Double? {
    if (bytes == null || bytes.size < 2) return null
    
    val int16 = ((bytes[1].toInt() and 0xFF) shl 8) or (bytes[0].toInt() and 0xFF)
    val sign = if (int16 shr 15 != 0) -1.0 else 1.0
    val exp = (int16 shr 10) and 0x1f
    val frac = int16 and 0x3ff
    
    return when (exp) {
        0 -> sign * Math.pow(2.0, -14.0) * (frac / 1024.0) // subnormals
        0x1f -> if (frac != 0) Double.NaN else sign * Double.POSITIVE_INFINITY
        else -> sign * Math.pow(2.0, (exp - 15).toDouble()) * (1 + frac / 1024.0)
    }
}