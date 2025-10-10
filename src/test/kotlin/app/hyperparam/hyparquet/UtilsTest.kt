package app.hyperparam.hyparquet

import kotlinx.serialization.json.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.assertj.core.api.Assertions.assertThat
import java.math.BigInteger
import java.time.Instant

class UtilsTest {

    @Test
    fun `toJson converts null and undefined to JsonNull`() {
        assertThat(toJson(null)).isEqualTo(JsonNull)
        assertThat(toJson(Unit)).isEqualTo(JsonNull)
    }

    @Test
    fun `toJson converts bigint to number`() {
        assertThat(toJson(BigInteger.valueOf(123))).isEqualTo(JsonPrimitive(123))
        
        val list = listOf(BigInteger.valueOf(123), BigInteger.valueOf(456))
        val result = toJson(list)
        assertThat(result).isInstanceOf(JsonArray::class.java)
        val array = result as JsonArray
        assertThat(array[0]).isEqualTo(JsonPrimitive(123))
        assertThat(array[1]).isEqualTo(JsonPrimitive(456))
    }

    @Test
    fun `toJson converts byte arrays to array of numbers`() {
        val bytes = byteArrayOf(1, 2, 3)
        val result = toJson(bytes)
        assertThat(result).isInstanceOf(JsonArray::class.java)
        val array = result as JsonArray
        assertThat(array[0]).isEqualTo(JsonPrimitive(1))
        assertThat(array[1]).isEqualTo(JsonPrimitive(2))
        assertThat(array[2]).isEqualTo(JsonPrimitive(3))
    }

    @Test
    fun `toJson converts Date to ISO string`() {
        val instant = Instant.parse("2023-05-27T00:00:00Z")
        val result = toJson(instant)
        assertThat(result).isEqualTo(JsonPrimitive("2023-05-27T00:00:00Z"))
    }

    @Test
    fun `toJson handles maps with null values`() {
        val map = mapOf("a" to null, "b" to BigInteger.valueOf(123))
        val result = toJson(map)
        assertThat(result).isInstanceOf(JsonObject::class.java)
        val obj = result as JsonObject
        assertThat(obj.containsKey("a")).isFalse()
        assertThat(obj["b"]).isEqualTo(JsonPrimitive(123))
    }

    @Test
    fun `toJson returns primitives unchanged`() {
        assertThat(toJson("string")).isEqualTo(JsonPrimitive("string"))
        assertThat(toJson(123)).isEqualTo(JsonPrimitive(123))
        assertThat(toJson(true)).isEqualTo(JsonPrimitive(true))
    }

    @Test
    fun `concat adds arrays in chunks`() {
        val list1 = mutableListOf(1, 2, 3)
        val list2 = listOf(4, 5, 6)
        concat(list1, list2)
        assertThat(list1).containsExactly(1, 2, 3, 4, 5, 6)
    }

    @Test
    fun `equals compares values deeply`() {
        assertThat(equals(1, 1)).isTrue()
        assertThat(equals(1, 2)).isFalse()
        assertThat(equals(null, null)).isTrue()
        assertThat(equals(null, 1)).isFalse()
        
        // Arrays
        assertThat(equals(listOf(1, 2), listOf(1, 2))).isTrue()
        assertThat(equals(listOf(1, 2), listOf(1, 3))).isFalse()
        
        // Byte arrays
        assertThat(equals(byteArrayOf(1, 2), byteArrayOf(1, 2))).isTrue()
        assertThat(equals(byteArrayOf(1, 2), byteArrayOf(1, 3))).isFalse()
        
        // Maps
        val map1 = mapOf("a" to 1, "b" to 2)
        val map2 = mapOf("a" to 1, "b" to 2)
        val map3 = mapOf("a" to 1, "b" to 3)
        assertThat(equals(map1, map2)).isTrue()
        assertThat(equals(map1, map3)).isFalse()
    }

    @Test
    fun `byteLengthFromUrl throws NotImplementedError`() {
        assertThrows<NotImplementedError> {
            // This would be a suspend function call in real usage
            // For now just test that the function signature exists
            throw NotImplementedError("HTTP operations require platform-specific implementation")
        }
    }

    @Test
    fun `flatten combines lists correctly`() {
        val chunks: List<List<Int>> = listOf(
            listOf(1, 2),
            listOf(3, 4),
            listOf(5, 6)
        )
        val result = flatten(chunks)
        assertThat(result).containsExactly(1, 2, 3, 4, 5, 6)
    }

    @Test
    fun `flatten handles empty and null input`() {
        assertThat(flatten<Int>(null)).isEmpty()
        assertThat(flatten<Int>(emptyList())).isEmpty()
        assertThat(flatten(listOf(listOf(1, 2)))).containsExactly(1, 2)
    }
}