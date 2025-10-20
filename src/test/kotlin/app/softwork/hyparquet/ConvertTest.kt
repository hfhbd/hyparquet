package app.softwork.hyparquet

import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConvertTest {
    
    private val name = "name"
    private val parsers = DEFAULT_PARSERS
    
    @Test
    fun `returns the same data if converted_type is undefined`() {
        val data = DecodedArray.IntArrayType(intArrayOf(1, 2, 3))
        val element = SchemaElement(name = name, logical_type = null)
        assertEquals(data, convert(data, ColumnDecoder(element = element, parsers = parsers)))
    }
    
    @Test
    fun `converts byte arrays to utf8`() {
        val data = DecodedArray.AnyArrayType(listOf("foo".toByteArray(), "bar".toByteArray()))
        val element = SchemaElement(name = name, converted_type = ConvertedType.UTF8, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(listOf("foo", "bar"), (result as DecodedArray.AnyArrayType).array)
    }
    
    @Test
    fun `converts byte arrays to utf8 default true`() {
        val data = DecodedArray.AnyArrayType(listOf("foo".toByteArray(), "bar".toByteArray()))
        val element = SchemaElement(name = name, type = ParquetType.BYTE_ARRAY, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(listOf("foo", "bar"), (result as DecodedArray.AnyArrayType).array)
    }
    
    @Test
    fun `preserves byte arrays utf8=false`() {
        val data = DecodedArray.AnyArrayType(listOf("foo".toByteArray(), "bar".toByteArray()))
        val element = SchemaElement(name = name, type = ParquetType.BYTE_ARRAY, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers, utf8 = false))
        assertTrue(result is DecodedArray.AnyArrayType)
        val arrays = (result as DecodedArray.AnyArrayType).array
        assertTrue(arrays[0] is ByteArray)
    }
    
    @Test
    fun `converts numbers to DECIMAL`() {
        val data = DecodedArray.IntArrayType(intArrayOf(100, 200))
        val element = SchemaElement(name = name, converted_type = ConvertedType.DECIMAL, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(listOf(100.0, 200.0), (result as DecodedArray.AnyArrayType).array)
    }
    
    @Test
    fun `converts numbers to DECIMAL with scale`() {
        val data = DecodedArray.IntArrayType(intArrayOf(100, 200))
        val element = SchemaElement(name = name, converted_type = ConvertedType.DECIMAL, scale = 2, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        val values = (result as DecodedArray.AnyArrayType).array
        assertEquals(1.0, values[0] as Double, 0.001)
        assertEquals(2.0, values[1] as Double, 0.001)
    }
    
    @Test
    fun `converts byte arrays to DECIMAL`() {
        val data = DecodedArray.AnyArrayType(listOf(
            byteArrayOf(0, 0, 0, 100),
            byteArrayOf(0, 0, 0, 200.toByte())
        ))
        val element = SchemaElement(name = name, converted_type = ConvertedType.DECIMAL, scale = 0, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        val values = (result as DecodedArray.AnyArrayType).array
        assertEquals(100.0, values[0] as Double, 0.001)
        assertEquals(200.0, values[1] as Double, 0.001)
    }
    
    @Test
    fun `converts epoch time to DATE`() {
        val data = DecodedArray.IntArrayType(intArrayOf(1, 2)) // days since epoch
        val element = SchemaElement(name = name, converted_type = ConvertedType.DATE, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        val dates = (result as DecodedArray.AnyArrayType).array
        assertEquals(Date(86400000L), dates[0])
        assertEquals(Date(86400000L * 2), dates[1])
    }
    
    @Test
    fun `converts epoch time to TIMESTAMP_MILLIS`() {
        val data = DecodedArray.LongArrayType(longArrayOf(1716506900000L, 1716507000000L))
        val element = SchemaElement(name = name, converted_type = ConvertedType.TIMESTAMP_MILLIS, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        // Just verify we get Date objects back
        val dates = (result as DecodedArray.AnyArrayType).array
        assertTrue(dates[0] is Date)
        assertTrue(dates[1] is Date)
    }
    
    @Test
    fun `converts epoch time to TIMESTAMP_MICROS`() {
        val data = DecodedArray.LongArrayType(longArrayOf(1716506900000000L, 1716507000000L))
        val element = SchemaElement(name = name, converted_type = ConvertedType.TIMESTAMP_MICROS, logical_type = null)
        val result = convert(data, ColumnDecoder(element = element, parsers = parsers))
        assertTrue(result is DecodedArray.AnyArrayType)
        // Just verify we get Date objects back
        val dates = (result as DecodedArray.AnyArrayType).array
        assertTrue(dates[0] is Date)
    }
    
    @Test
    fun `throws error for BSON conversion`() {
        val data = DecodedArray.AnyArrayType(listOf(emptyMap<String, Any>()))
        val element = SchemaElement(name = name, converted_type = ConvertedType.BSON, logical_type = null)
        assertFailsWith<Error> {
            convert(data, ColumnDecoder(element = element, parsers = parsers))
        }
    }
    
    @Test
    fun `throws error for INTERVAL conversion`() {
        val data = DecodedArray.AnyArrayType(listOf(emptyMap<String, Any>()))
        val element = SchemaElement(name = name, converted_type = ConvertedType.INTERVAL, logical_type = null)
        assertFailsWith<Error> {
            convert(data, ColumnDecoder(element = element, parsers = parsers))
        }
    }
    
    @Test
    fun `parseDecimal handles various byte arrays`() {
        assertEquals(100.0, parseDecimal(byteArrayOf(0, 0, 0, 100)))
        assertEquals(200.0, parseDecimal(byteArrayOf(0, 0, 0, 200.toByte())))
        assertEquals(0.0, parseDecimal(byteArrayOf()))
    }
    
    @Test
    fun `parseFloat16 handles various inputs`() {
        assertEquals(1.0, parseFloat16(byteArrayOf(0x00, 0x3c)) ?: 0.0, 0.01)
        assertEquals(2.0, parseFloat16(byteArrayOf(0x00, 0x40)) ?: 0.0, 0.01)
        assertEquals(null, parseFloat16(null))
    }
}
