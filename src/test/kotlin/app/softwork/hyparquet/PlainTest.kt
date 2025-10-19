package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PlainTest {
    
    @Test
    fun `returns empty array for count 0`() {
        val view = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val reader = DataReader(view, 0)
        val result = readPlain(reader, ParquetType.INT32, 0, null)
        assertTrue(result is DecodedArray.AnyArrayType)
        assertEquals(0, (result as DecodedArray.AnyArrayType).array.size)
        assertEquals(0, reader.offset)
    }
    
    @Test
    fun `reads BOOLEAN values`() {
        val buffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0b00000101.toByte()) // true, false, true
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.BOOLEAN, 3, null)
        assertTrue(result is DecodedArray.AnyArrayType)
        val values = (result as DecodedArray.AnyArrayType).array
        assertEquals(listOf(true, false, true), values)
        assertEquals(1, reader.offset)
    }
    
    @Test
    fun `reads INT32 values`() {
        val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(123456789)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.INT32, 1, null)
        assertTrue(result is DecodedArray.IntArrayType)
        assertEquals(123456789, (result as DecodedArray.IntArrayType).array[0])
        assertEquals(4, reader.offset)
    }
    
    @Test
    fun `reads INT64 values`() {
        val buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putLong(1234567890123456789L)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.INT64, 1, null)
        assertTrue(result is DecodedArray.LongArrayType)
        assertEquals(1234567890123456789L, (result as DecodedArray.LongArrayType).array[0])
        assertEquals(8, reader.offset)
    }
    
    @Test
    fun `reads FLOAT values`() {
        val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putFloat(1234.5f)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.FLOAT, 1, null)
        assertTrue(result is DecodedArray.FloatArrayType)
        assertEquals(1234.5f, (result as DecodedArray.FloatArrayType).array[0], 0.001f)
        assertEquals(4, reader.offset)
    }
    
    @Test
    fun `reads DOUBLE values`() {
        val buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putDouble(12345.67)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.DOUBLE, 1, null)
        assertTrue(result is DecodedArray.DoubleArrayType)
        assertEquals(12345.67, (result as DecodedArray.DoubleArrayType).array[0], 0.001)
        assertEquals(8, reader.offset)
    }
    
    @Test
    fun `reads BYTE_ARRAY values`() {
        val buffer = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN)
        val str1 = "Hello"
        buffer.putInt(str1.length)
        buffer.put(str1.toByteArray())
        val str2 = "World"
        buffer.putInt(str2.length)
        buffer.put(str2.toByteArray())
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.BYTE_ARRAY, 2, null)
        assertTrue(result is DecodedArray.AnyArrayType)
        val arrays = (result as DecodedArray.AnyArrayType).array
        assertEquals(2, arrays.size)
        assertEquals("Hello", (arrays[0] as ByteArray).decodeToString())
        assertEquals("World", (arrays[1] as ByteArray).decodeToString())
    }
    
    @Test
    fun `reads FIXED_LEN_BYTE_ARRAY values`() {
        val buffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(byteArrayOf(1, 2, 3))
        buffer.put(byteArrayOf(4, 5, 6))
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val result = readPlain(reader, ParquetType.FIXED_LEN_BYTE_ARRAY, 2, 3)
        assertTrue(result is DecodedArray.AnyArrayType)
        val arrays = (result as DecodedArray.AnyArrayType).array
        assertEquals(2, arrays.size)
        assertTrue((arrays[0] as ByteArray).contentEquals(byteArrayOf(1, 2, 3)))
        assertTrue((arrays[1] as ByteArray).contentEquals(byteArrayOf(4, 5, 6)))
    }
}
