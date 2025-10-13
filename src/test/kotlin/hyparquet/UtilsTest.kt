package hyparquet

import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.*

class UtilsTest {

    @Test
    fun `toJson converts null to JsonNull`() {
        assertEquals(JsonNull, toJson(null))
    }

    @Test
    fun `toJson converts numbers correctly`() {
        assertEquals(JsonPrimitive(123), toJson(123))
        assertEquals(JsonPrimitive(123L), toJson(123L))
        assertEquals(JsonPrimitive(123.45), toJson(123.45))
    }

    @Test
    fun `toJson converts ByteArray to array of numbers`() {
        val byteArray = byteArrayOf(1, 2, 3)
        val expected = JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(2), JsonPrimitive(3)))
        assertEquals(expected, toJson(byteArray))
    }

    @Test
    fun `toJson converts Date to ISO string`() {
        val date = Date(1685145600000L) // 2023-05-27T00:00:00Z
        val result = toJson(date)
        assertTrue(result is JsonPrimitive)
        assertTrue((result as JsonPrimitive).content.contains("2023"))
    }

    @Test
    fun `toJson converts lists correctly`() {
        val list = listOf(1, 2, 3)
        val expected = JsonArray(listOf(JsonPrimitive(1), JsonPrimitive(2), JsonPrimitive(3)))
        assertEquals(expected, toJson(list))
    }

    @Test
    fun `toJson converts maps correctly`() {
        val map = mapOf("a" to 123, "b" to "test")
        val result = toJson(map)
        assertTrue(result is JsonObject)
        val obj = result as JsonObject
        assertEquals(JsonPrimitive(123), obj["a"])
        assertEquals(JsonPrimitive("test"), obj["b"])
    }

    @Test
    fun `DecodedArray length property works`() {
        val uint8Array = DecodedArray.Uint8(byteArrayOf(1, 2, 3))
        assertEquals(3, uint8Array.length)

        val intArray = DecodedArray.Int32(intArrayOf(1, 2, 3, 4))
        assertEquals(4, intArray.length)

        val anyList = DecodedArray.AnyList(listOf(1, 2, 3, 4, 5))
        assertEquals(5, anyList.length)
    }

    @Test
    fun `DecodedArray toList works`() {
        val uint8Array = DecodedArray.Uint8(byteArrayOf(1, 2, 3))
        assertEquals(listOf(1, 2, 3), uint8Array.toList())

        val intArray = DecodedArray.Int32(intArrayOf(1, 2, 3))
        assertEquals(listOf(1, 2, 3), intArray.toList())

        val anyList = DecodedArray.AnyList(listOf("a", "b", "c"))
        assertEquals(listOf("a", "b", "c"), anyList.toList())
    }

    @Test
    fun `DefaultParsers works`() {
        // Test timestamp conversion
        val timestamp = DefaultParsers.timestampFromMilliseconds(1685145600000L)
        assertTrue(timestamp is Date)

        // Test date from days
        val date = DefaultParsers.dateFromDays(1)
        assertTrue(date is Date)

        // Test string from bytes
        val string = DefaultParsers.stringFromBytes("hello".toByteArray())
        assertEquals("hello", string)
    }
}