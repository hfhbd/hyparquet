package io.github.hfhbd.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class UtilsTest {

    @Test
    fun testToJson() {
        assertEquals(null, toJson(null))
        assertEquals(42L, toJson(42L))
        assertEquals(listOf(1, 2, 3), toJson(listOf(1, 2, 3)))
        assertEquals(listOf<Byte>(1, 2, 3), toJson(byteArrayOf(1, 2, 3)))
        
        val map = mapOf("key" to "value", "null_key" to null)
        val result = toJson(map) as Map<*, *>
        assertEquals("value", result["key"])
        assertEquals(1, result.size) // null values should be filtered out
    }

    @Test
    fun testConcat() {
        val list1 = mutableListOf(1, 2, 3)
        val list2 = listOf(4, 5, 6)
        list1.concat(list2)
        assertEquals(listOf(1, 2, 3, 4, 5, 6), list1)
    }

    @Test
    fun testDeepEquals() {
        assertTrue(deepEquals(null, null))
        assertTrue(deepEquals(1, 1))
        assertTrue(deepEquals("test", "test"))
        assertTrue(deepEquals(listOf(1, 2, 3), listOf(1, 2, 3)))
        assertTrue(deepEquals(mapOf("a" to 1), mapOf("a" to 1)))
        assertTrue(deepEquals(byteArrayOf(1, 2, 3), byteArrayOf(1, 2, 3)))
        
        assertTrue(!deepEquals(1, 2))
        assertTrue(!deepEquals(listOf(1, 2), listOf(1, 3)))
        assertTrue(!deepEquals(mapOf("a" to 1), mapOf("a" to 2)))
    }

    @Test
    fun testByteArrayAsyncBuffer() {
        val data = byteArrayOf(1, 2, 3, 4, 5)
        val buffer = ByteArrayAsyncBuffer(data)
        assertEquals(5, buffer.byteLength)
        assertNotNull(buffer)
    }

    @Test
    fun testFlatten() {
        val nested = listOf(
            listOf(1, 2),
            listOf(3, 4),
            listOf(5)
        )
        assertEquals(listOf(1, 2, 3, 4, 5), flatten(nested))
    }
}

class ConvertTest {

    @Test
    fun testParseDecimal() {
        assertEquals(0.0, parseDecimal(byteArrayOf()))
        assertEquals(100.0, parseDecimal(byteArrayOf(0, 0, 0, 100)))
        
        // Test signed values
        val negativeBytes = byteArrayOf(-1, -1, -1, -100)
        val result = parseDecimal(negativeBytes)
        assertTrue(result < 0)
    }

    @Test
    fun testParseFloat16() {
        // Test null input
        assertEquals(null, parseFloat16(null))
        assertEquals(null, parseFloat16(byteArrayOf()))
        
        // Test basic float16 parsing
        val bytes = byteArrayOf(0, 60) // This represents 1.0 in float16
        val result = parseFloat16(bytes)
        assertNotNull(result)
        assertTrue(result!! > 0.9 && result < 1.1) // Approximately 1.0
    }

    @Test
    fun testDefaultParsers() {
        val parsers = DefaultParsers
        
        // Test timestamp parsing
        val timestamp = parsers.timestampFromMilliseconds(1000L)
        assertNotNull(timestamp)
        
        val date = parsers.dateFromDays(1)
        assertNotNull(date)
        
        val string = parsers.stringFromBytes("test".toByteArray())
        assertEquals("test", string)
    }
}

class TypesTest {

    @Test
    fun testEnumValues() {
        // Test that our enums have the expected values
        assertTrue(ParquetType.values().contains(ParquetType.BOOLEAN))
        assertTrue(ParquetType.values().contains(ParquetType.INT32))
        assertTrue(ParquetType.values().contains(ParquetType.BYTE_ARRAY))
        
        assertTrue(ConvertedType.values().contains(ConvertedType.UTF8))
        assertTrue(ConvertedType.values().contains(ConvertedType.DECIMAL))
        
        assertTrue(TimeUnit.values().contains(TimeUnit.MILLIS))
        assertTrue(TimeUnit.values().contains(TimeUnit.MICROS))
        assertTrue(TimeUnit.values().contains(TimeUnit.NANOS))
    }

    @Test
    fun testLogicalTypeHierarchy() {
        val stringType = LogicalType.STRING
        val decimalType = LogicalType.DECIMAL(10, 2)
        val timestampType = LogicalType.TIMESTAMP(true, TimeUnit.MILLIS)
        
        assertNotNull(stringType)
        assertNotNull(decimalType)
        assertNotNull(timestampType)
        
        // Test sealed class properties
        assertEquals(10, decimalType.precision)
        assertEquals(2, decimalType.scale)
        assertEquals(true, timestampType.isAdjustedToUTC)
        assertEquals(TimeUnit.MILLIS, timestampType.unit)
    }

    @Test
    fun testDataClasses() {
        val keyValue = KeyValue("test_key", "test_value")
        assertEquals("test_key", keyValue.key)
        assertEquals("test_value", keyValue.value)
        
        val columnData = ColumnData("column1", listOf(1, 2, 3), 0, 3)
        assertEquals("column1", columnData.columnName)
        assertEquals(listOf(1, 2, 3), columnData.columnData)
        assertEquals(0, columnData.rowStart)
        assertEquals(3, columnData.rowEnd)
    }
}