package app.hyperparam.hyparquet

import kotlinx.serialization.json.*
import java.math.BigInteger
import java.nio.ByteBuffer
import java.time.Instant
import java.time.format.DateTimeFormatter

/**
 * Replace bigint, date, etc with legal JSON types.
 */
fun toJson(obj: Any?): JsonElement {
    return when (obj) {
        null, Unit -> JsonNull
        is Boolean -> JsonPrimitive(obj)
        is Number -> JsonPrimitive(obj)
        is BigInteger -> JsonPrimitive(obj.toLong())
        is String -> JsonPrimitive(obj)
        is Instant -> JsonPrimitive(obj.toString())
        is ByteArray -> JsonArray(obj.map { JsonPrimitive(it.toInt() and 0xFF) })
        is UByteArray -> JsonArray(obj.map { JsonPrimitive(it.toInt()) })
        is Array<*> -> JsonArray(obj.map { toJson(it) })
        is List<*> -> JsonArray(obj.map { toJson(it) })
        is Map<*, *> -> {
            val jsonObj = mutableMapOf<String, JsonElement>()
            obj.forEach { (key, value) ->
                if (value != null) {
                    jsonObj[key.toString()] = toJson(value)
                }
            }
            JsonObject(jsonObj)
        }
        else -> {
            // For general objects, try to convert their properties
            JsonPrimitive(obj.toString())
        }
    }
}

/**
 * Concatenate two arrays fast.
 */
fun <T> concat(aaa: MutableList<T>, bbb: List<T>) {
    val chunk = 10000
    var i = 0
    while (i < bbb.size) {
        val endIndex = minOf(i + chunk, bbb.size)
        aaa.addAll(bbb.subList(i, endIndex))
        i += chunk
    }
}

/**
 * Deep equality comparison
 */
fun equals(a: Any?, b: Any?): Boolean {
    if (a === b) return true
    
    when {
        a is ByteArray && b is ByteArray -> return a.contentEquals(b)
        a is UByteArray && b is UByteArray -> return a.contentEquals(b)
        a == null || b == null || a.javaClass != b.javaClass -> return false
        a is Array<*> && b is Array<*> -> {
            if (a.size != b.size) return false
            return a.indices.all { equals(a[it], b[it]) }
        }
        a is List<*> && b is List<*> -> {
            if (a.size != b.size) return false
            return a.indices.all { equals(a[it], b[it]) }
        }
        a is Map<*, *> && b is Map<*, *> -> {
            if (a.size != b.size) return false
            return a.keys.all { key -> equals(a[key], b[key]) }
        }
        else -> return a == b
    }
}

/**
 * Get the byte length of a URL using a HEAD request.
 */
suspend fun byteLengthFromUrl(
    url: String,
    headers: Map<String, String> = emptyMap()
): Long {
    // Note: This would require HTTP client implementation in a real Kotlin project
    // For now, throwing an exception to indicate this needs platform-specific implementation
    throw NotImplementedError("HTTP operations require platform-specific implementation")
}

/**
 * Interface equivalent to JavaScript's AsyncBuffer
 */
interface AsyncBuffer {
    val byteLength: Long
    suspend fun slice(start: Long, end: Long? = null): ByteArray
}

/**
 * Construct an AsyncBuffer for a URL.
 */
suspend fun asyncBufferFromUrl(
    url: String,
    byteLength: Long? = null,
    headers: Map<String, String> = emptyMap()
): AsyncBuffer {
    // Note: This would require HTTP client implementation in a real Kotlin project
    throw NotImplementedError("HTTP operations require platform-specific implementation")
}

/**
 * Returns a cached layer on top of an AsyncBuffer.
 */
fun cachedAsyncBuffer(
    file: AsyncBuffer,
    minSize: Long = 524288 // Default 512KB like JavaScript version
): AsyncBuffer {
    if (file.byteLength < minSize) {
        // Cache whole file if it's small - we need to handle the suspend function
        return object : AsyncBuffer {
            override val byteLength = file.byteLength
            override suspend fun slice(start: Long, end: Long?): ByteArray {
                val buffer = file.slice(0, file.byteLength)
                val endPos = end ?: buffer.size.toLong()
                return buffer.sliceArray(start.toInt() until endPos.toInt())
            }
        }
    }
    
    val cache = mutableMapOf<String, ByteArray>()
    return object : AsyncBuffer {
        override val byteLength = file.byteLength
        override suspend fun slice(start: Long, end: Long?): ByteArray {
            val key = cacheKey(start, end, byteLength)
            return cache.getOrPut(key) {
                file.slice(start, end)
            }
        }
    }
}

/**
 * Returns canonical cache key for a byte range 'start,end'.
 */
private fun cacheKey(start: Long, end: Long?, size: Long?): String {
    return when {
        start < 0 -> {
            if (end != null) throw IllegalArgumentException("invalid suffix range [$start, $end]")
            if (size == null) "$start,"
            else "${size + start},$size"
        }
        end != null -> {
            if (start > end) throw IllegalArgumentException("invalid empty range [$start, $end]")
            "$start,$end"
        }
        size == null -> "$start,"
        else -> "$start,$size"
    }
}

/**
 * Flatten a list of lists into a single list.
 */
fun <T> flatten(chunks: List<List<T>>?): List<T> {
    if (chunks.isNullOrEmpty()) return emptyList()
    if (chunks.size == 1) return chunks[0]
    
    val output = mutableListOf<T>()
    for (chunk in chunks) {
        output.addAll(chunk)
    }
    return output
}