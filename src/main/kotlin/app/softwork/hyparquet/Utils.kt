package app.softwork.hyparquet

import kotlinx.io.bytestring.encodeToByteString
import kotlinx.serialization.json.*
import kotlin.time.Duration

/**
 * Replace bigint, date, etc. with legal JSON types.
 *
 * @param obj object to convert
 * @returns converted object
 */
fun toJson(obj: Any?): JsonElement = when (obj) {
    null -> JsonNull
    is Boolean -> JsonPrimitive(obj)
    is Number -> JsonPrimitive(obj)
    is String -> JsonPrimitive(obj)
    is ByteArray -> JsonArray(obj.map { JsonPrimitive(it) })
    is List<*> -> JsonArray(obj.map { toJson(it) })
    is Map<*, *> -> buildJsonObject {
        obj.forEach { (key, value) ->
            if (value != null) {
                put(key.toString(), toJson(value))
            }
        }
    }
    else -> JsonPrimitive(obj.toString())
}

/**
 * Concatenate two arrays fast.
 */
fun concat(aaa: MutableList<Any?>, bbb: DecodedArray) {
    val chunk = 10000
    when (bbb) {
        is DecodedArray.AnyArrayType -> aaa.addAll(bbb.array)
        is DecodedArray.ByteArrayType -> {
            for (i in bbb.array.indices step chunk) {
                aaa.addAll(bbb.array.sliceArray(i until minOf(i + chunk, bbb.array.size)).toList())
            }
        }
        is DecodedArray.IntArrayType -> {
            for (i in bbb.array.indices step chunk) {
                aaa.addAll(bbb.array.sliceArray(i until minOf(i + chunk, bbb.array.size)).toList())
            }
        }
        is DecodedArray.LongArrayType -> {
            for (i in bbb.array.indices step chunk) {
                aaa.addAll(bbb.array.sliceArray(i until minOf(i + chunk, bbb.array.size)).toList())
            }
        }
        is DecodedArray.FloatArrayType -> {
            for (i in bbb.array.indices step chunk) {
                aaa.addAll(bbb.array.sliceArray(i until minOf(i + chunk, bbb.array.size)).toList())
            }
        }
        is DecodedArray.DoubleArrayType -> {
            for (i in bbb.array.indices step chunk) {
                aaa.addAll(bbb.array.sliceArray(i until minOf(i + chunk, bbb.array.size)).toList())
            }
        }
    }
}

/**
 * Flatten a list of lists into a single list.
 */
fun flatten(chunks: List<DecodedArray>?): DecodedArray {
    if (chunks == null) return DecodedArray.AnyArrayType(emptyList())
    if (chunks.size == 1) return chunks[0]
    
    val output = mutableListOf<Any?>()
    for (chunk in chunks) {
        concat(output, chunk)
    }
    return DecodedArray.AnyArrayType(output)
}
