package io.github.hfhbd.hyparquet

import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import java.text.SimpleDateFormat
import java.util.*
import java.io.InputStream
import java.net.URL
import java.net.HttpURLConnection

/**
 * Replace bigint, date, etc with legal JSON types.
 */
fun toJson(obj: Any?): Any? {
    return when (obj) {
        null -> null
        is Long -> obj
        is List<*> -> obj.map { toJson(it) }
        is ByteArray -> obj.toList()
        is Date -> SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }.format(obj)
        is Map<*, *> -> {
            obj.mapNotNull { (key, value) ->
                if (value != null) key.toString() to toJson(value) else null
            }.toMap()
        }
        else -> obj
    }
}

/**
 * Concatenate two arrays fast.
 */
fun <T> MutableList<T>.concat(other: List<T>) {
    val chunkSize = 10000
    for (i in other.indices step chunkSize) {
        val end = minOf(i + chunkSize, other.size)
        this.addAll(other.subList(i, end))
    }
}

/**
 * Deep equality comparison
 */
fun deepEquals(a: Any?, b: Any?): Boolean {
    if (a === b) return true
    if (a is ByteArray && b is ByteArray) return a.contentEquals(b)
    if (a == null || b == null || a::class != b::class) return false
    
    return when {
        a is List<*> && b is List<*> -> {
            a.size == b.size && a.indices.all { deepEquals(a[it], b[it]) }
        }
        a is Map<*, *> && b is Map<*, *> -> {
            a.size == b.size && a.keys.all { key -> 
                b.containsKey(key) && deepEquals(a[key], b[key])
            }
        }
        else -> a == b
    }
}

/**
 * Get the byte length of a URL using a HEAD request.
 */
suspend fun byteLengthFromUrl(
    url: String,
    headers: Map<String, String> = emptyMap()
): Int {
    val connection = URL(url).openConnection() as HttpURLConnection
    try {
        connection.requestMethod = "HEAD"
        headers.forEach { (key, value) ->
            connection.setRequestProperty(key, value)
        }
        
        if (connection.responseCode != 200) {
            throw RuntimeException("HEAD request failed with status ${connection.responseCode}")
        }
        
        val contentLength = connection.getHeaderField("Content-Length")
            ?: throw RuntimeException("Missing Content-Length header")
        
        return contentLength.toInt()
    } finally {
        connection.disconnect()
    }
}

/**
 * Construct an AsyncBuffer for a URL.
 */
suspend fun asyncBufferFromUrl(
    url: String,
    byteLength: Int? = null,
    headers: Map<String, String> = emptyMap()
): AsyncBuffer {
    if (url.isEmpty()) throw IllegalArgumentException("Missing URL")
    
    val actualByteLength = byteLength ?: byteLengthFromUrl(url, headers)
    var buffer: ByteArray? = null

    return object : AsyncBuffer {
        override val byteLength: Int = actualByteLength

        override suspend fun slice(start: Int, end: Int?): ByteArray {
            // If we already have the full buffer, use it
            buffer?.let { buf ->
                val actualEnd = end ?: buf.size
                return buf.sliceArray(start until actualEnd)
            }

            val connection = URL(url).openConnection() as HttpURLConnection
            try {
                headers.forEach { (key, value) ->
                    connection.setRequestProperty(key, value)
                }

                val actualEnd = end ?: actualByteLength
                val endByte = if (end == null) "" else "${actualEnd - 1}"
                connection.setRequestProperty("Range", "bytes=$start-$endByte")

                when (connection.responseCode) {
                    200 -> {
                        // Endpoint doesn't support range requests, got full object
                        buffer = connection.inputStream.readBytes()
                        return buffer!!.sliceArray(start until actualEnd)
                    }
                    206 -> {
                        // Endpoint supports range requests, got requested range
                        return connection.inputStream.readBytes()
                    }
                    else -> {
                        throw RuntimeException("Unexpected response code: ${connection.responseCode}")
                    }
                }
            } finally {
                connection.disconnect()
            }
        }
    }
}

/**
 * Returns a cached layer on top of an AsyncBuffer.
 */
fun cachedAsyncBuffer(
    file: AsyncBuffer,
    minSize: Int = 8192  // defaultInitialFetchSize equivalent
): AsyncBuffer {
    if (file.byteLength < minSize) {
        // Cache whole file if it's small
        val cachedBuffer: ByteArray by lazy {
            runBlocking {
                file.slice(0, file.byteLength)
            }
        }
        
        return object : AsyncBuffer {
            override val byteLength: Int = file.byteLength

            override suspend fun slice(start: Int, end: Int?): ByteArray {
                val actualEnd = end ?: cachedBuffer.size
                return cachedBuffer.sliceArray(start until actualEnd)
            }
        }
    }
    
    val cache = mutableMapOf<String, ByteArray>()
    
    return object : AsyncBuffer {
        override val byteLength: Int = file.byteLength

        override suspend fun slice(start: Int, end: Int?): ByteArray {
            val key = cacheKey(start, end, byteLength)
            cache[key]?.let { return it }
            
            val result = file.slice(start, end)
            cache[key] = result
            return result
        }
    }
}

private fun cacheKey(start: Int, end: Int?, byteLength: Int): String {
    val actualEnd = end ?: byteLength
    return "$start-$actualEnd"
}

/**
 * Flatten nested arrays into a single array
 */
fun <T> flatten(arrays: List<List<T>>): List<T> {
    return arrays.flatten()
}

/**
 * AsyncBuffer implementation for byte arrays
 */
class ByteArrayAsyncBuffer(private val data: ByteArray) : AsyncBuffer {
    override val byteLength: Int = data.size

    override suspend fun slice(start: Int, end: Int?): ByteArray {
        val actualEnd = end ?: data.size
        return data.sliceArray(start until actualEnd)
    }
}

/**
 * AsyncBuffer implementation for input streams
 */
class InputStreamAsyncBuffer(
    private val inputStream: InputStream,
    override val byteLength: Int
) : AsyncBuffer {
    private val data: ByteArray by lazy { inputStream.readBytes() }

    override suspend fun slice(start: Int, end: Int?): ByteArray {
        val actualEnd = end ?: data.size
        return data.sliceArray(start until actualEnd)
    }
}