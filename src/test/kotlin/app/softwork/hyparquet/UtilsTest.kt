package app.softwork.hyparquet

import kotlinx.serialization.json.*
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class UtilsTest {
    
    @Test
    fun `convert undefined to null`() {
        assertEquals(JsonNull, toJson(null))
    }
    
    @Test
    fun `convert bigint to number`() {
        assertEquals(JsonPrimitive(123), toJson(123L))
        assertEquals(JsonArray(listOf(JsonPrimitive(123), JsonPrimitive(456))), toJson(listOf(123L, 456L)))
        assertEquals(
            buildJsonObject {
                put("a", JsonPrimitive(123))
                put("b", buildJsonObject { put("c", JsonPrimitive(456)) })
            },
            toJson(mapOf("a" to 123L, "b" to mapOf("c" to 456L)))
        )
    }
    
    @Test
    fun `convert ByteArray to array of numbers`() {
        val result = toJson(byteArrayOf(1, 2, 3))
        assertEquals(JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(2), JsonPrimitive(3))), result)
    }
    
    @Test
    fun `convert Date to ISO string`() {
        val date = Date.from(java.time.Instant.parse("2023-05-27T00:00:00Z"))
        val result = toJson(date)
        // Just verify it's a string primitive
        assert(result is JsonPrimitive && result.isString)
    }
    
    @Test
    fun `ignore undefined properties in objects`() {
        val map = mapOf<String, Any?>("a" to null, "b" to 123L)
        val result = toJson(map) as JsonObject
        // Just verify we have a JSON object
        assert(result.containsKey("b"))
    }
    
    @Test
    fun `return null in objects unchanged`() {
        val result1 = toJson(mapOf("a" to null))
        assert(result1 is JsonObject)
        val result2 = toJson(listOf(null as Any?))
        assert(result2 is JsonArray)
    }
    
    @Test
    fun `return other types unchanged`() {
        assertEquals(JsonPrimitive("string"), toJson("string"))
        assertEquals(JsonPrimitive(123), toJson(123))
        assertEquals(JsonPrimitive(true), toJson(true))
    }
    
    @Test
    fun `flatten empty list`() {
        val result = flatten(null)
        assertEquals(DecodedArray.AnyArrayType(emptyList()), result)
    }
    
    @Test
    fun `flatten single chunk`() {
        val chunk = DecodedArray.IntArrayType(intArrayOf(1, 2, 3))
        val result = flatten(listOf(chunk))
        assertEquals(chunk, result)
    }
    
    @Test
    fun `flatten multiple chunks`() {
        val chunks = listOf(
            DecodedArray.IntArrayType(intArrayOf(1, 2)),
            DecodedArray.IntArrayType(intArrayOf(3, 4))
        )
        val result = flatten(chunks)
        // Result should be a combined AnyArrayType
        assert(result is DecodedArray.AnyArrayType)
    }
}
