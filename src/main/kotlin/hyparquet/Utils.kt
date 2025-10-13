package hyparquet

import kotlinx.serialization.json.*
import java.time.Instant
import java.util.*

/**
 * Replace bigint, date, etc. with legal JSON types.
 */
fun toJson(obj: Any?): JsonElement = when (obj) {
    null -> JsonNull
    is Boolean -> JsonPrimitive(obj)
    is Number -> JsonPrimitive(obj)
    is String -> JsonPrimitive(obj)
    is ByteArray -> JsonArray(obj.map { JsonPrimitive(it.toInt() and 0xFF) })
    is Date -> JsonPrimitive(obj.toInstant().toString())
    is Instant -> JsonPrimitive(obj.toString())
    is List<*> -> JsonArray(obj.map { toJson(it) })
    is Array<*> -> JsonArray(obj.map { toJson(it) })
    is Map<*, *> -> JsonObject(
        obj.mapNotNull { (k, v) ->
            val key = k?.toString()
            if (key != null && v != null) key to toJson(v) else null
        }.toMap()
    )
    else -> {
        // Try to handle data classes and other objects via reflection
        try {
            val fields = obj::class.java.declaredFields
            val jsonMap = mutableMapOf<String, JsonElement>()
            for (field in fields) {
                field.isAccessible = true
                val value = field.get(obj)
                if (value != null) {
                    jsonMap[field.name] = toJson(value)
                }
            }
            JsonObject(jsonMap)
        } catch (e: Exception) {
            JsonPrimitive(obj.toString())
        }
    }
}

/**
 * Concatenate two lists fast.
 */
fun <T> MutableList<T>.concatList(other: List<T>) {
    val chunk = 10000
    for (i in other.indices step chunk) {
        val endIndex = minOf(i + chunk, other.size)
        addAll(other.subList(i, endIndex))
    }
}

/**
 * Flatten a list of lists into a single list.
 */
fun <T> List<List<T>>.flatten(): List<T> {
    return this.flatten()
}

/**
 * Convert DecodedArray to a regular list
 */
fun DecodedArray.toList(): List<Any?> = when (this) {
    is DecodedArray.Uint8 -> array.map { it.toInt() and 0xFF }
    is DecodedArray.Uint32 -> array.toList()
    is DecodedArray.Int32 -> array.toList()
    is DecodedArray.BigInt64 -> array.toList()
    is DecodedArray.BigUint64 -> array.toList()
    is DecodedArray.Float32 -> array.toList()
    is DecodedArray.Float64 -> array.toList()
    is DecodedArray.AnyList -> array
}

/**
 * Get the length of a DecodedArray
 */
val DecodedArray.length: Int
    get() = when (this) {
        is DecodedArray.Uint8 -> array.size
        is DecodedArray.Uint32 -> array.size
        is DecodedArray.Int32 -> array.size
        is DecodedArray.BigInt64 -> array.size
        is DecodedArray.BigUint64 -> array.size
        is DecodedArray.Float32 -> array.size
        is DecodedArray.Float64 -> array.size
        is DecodedArray.AnyList -> array.size
    }

/**
 * Default implementation of ParquetParsers
 */
object DefaultParsers : ParquetParsers {
    override fun timestampFromMilliseconds(millis: Long): Any {
        return Date(millis)
    }

    override fun timestampFromMicroseconds(micros: Long): Any {
        return Date(micros / 1000)
    }

    override fun timestampFromNanoseconds(nanos: Long): Any {
        return Date(nanos / 1_000_000)
    }

    override fun dateFromDays(days: Int): Any {
        val epochDay = Date(0L) // Unix epoch
        val calendar = Calendar.getInstance()
        calendar.time = epochDay
        calendar.add(Calendar.DAY_OF_YEAR, days)
        return calendar.time
    }

    override fun stringFromBytes(bytes: ByteArray): Any {
        return String(bytes, Charsets.UTF_8)
    }
}