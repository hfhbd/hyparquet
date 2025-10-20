package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class EncodingTest {
    
    @Test
    fun `reads RLE values with explicit length`() {
        val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        // RLE 3x true
        buffer.put(0b00000110.toByte())
        buffer.put(1.toByte())
        // RLE 3x 100
        buffer.put(0b00000110.toByte())
        buffer.put(100.toByte())
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(6)
        readRleBitPackedHybrid(reader, 1, values, 4)
        assertEquals(4, reader.offset)
        assertEquals(listOf(1, 1, 1, 100, 100, 100), values.toList())
    }
    
    @Test
    fun `reads RLE values with bitwidth=16`() {
        val buffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN)
        buffer.position(3)
        buffer.put(0b00000110.toByte())
        buffer.putShort(65535.toShort())
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(3)
        readRleBitPackedHybrid(reader, 16, values, 6)
        assertEquals(6, reader.offset)
        assertEquals(listOf(65535, 65535, 65535), values.toList())
    }
    
    @Test
    fun `reads RLE values with bitwidth=24`() {
        val buffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0b00000100.toByte())
        buffer.put(255.toByte())
        buffer.put(255.toByte())
        buffer.put(255.toByte())
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(2)
        readRleBitPackedHybrid(reader, 24, values, 4)
        assertEquals(4, reader.offset)
        assertEquals(listOf(16777215, 16777215), values.toList())
    }
    
    @Test
    fun `reads RLE values with bitwidth=32`() {
        val buffer = ByteBuffer.allocate(5).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0b00000110.toByte())
        buffer.putInt(234000)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(3)
        readRleBitPackedHybrid(reader, 32, values, 5)
        assertEquals(5, reader.offset)
        assertEquals(listOf(234000, 234000, 234000), values.toList())
    }
    
    @Test
    fun `reads bit-packed values with implicit length`() {
        val buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(2) // length 2 little-endian
        buffer.put(0b00000011.toByte()) // Bit-packed header for 1-8 values
        buffer.put(0b00000100.toByte()) // Bit-packed values (false, false, true)
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(3)
        readRleBitPackedHybrid(reader, 1, values)
        assertEquals(6, reader.offset)
        assertEquals(listOf(0, 0, 1), values.toList())
    }
    
    @Test
    fun `reads multi-byte bit-packed values`() {
        val buffer = ByteBuffer.allocate(3).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0b00000101.toByte()) // Bit-packed header for 9-16 values
        buffer.put(0b11111111.toByte())
        buffer.put(0b00000001.toByte())
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(9)
        readRleBitPackedHybrid(reader, 1, values, 3)
        assertEquals(3, reader.offset)
        assertEquals(listOf(1, 1, 1, 1, 1, 1, 1, 1, 1), values.toList())
    }
    
    @Test
    fun `throws for invalid bit-packed offset`() {
        val buffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN)
        buffer.put(0b00000011.toByte()) // Bit-packed header for 3 values
        buffer.flip()
        val reader = DataReader(buffer, 0)
        
        val values = IntArray(3)
        assertFailsWith<Error> {
            readRleBitPackedHybrid(reader, 1, values, 3)
        }
    }
    
    @Test
    fun `calculates bit widths`() {
        assertEquals(0, bitWidth(0))
        assertEquals(1, bitWidth(1))
        assertEquals(3, bitWidth(7))
        assertEquals(4, bitWidth(8))
        assertEquals(8, bitWidth(255))
        assertEquals(9, bitWidth(256))
        assertEquals(10, bitWidth(1023))
        assertEquals(20, bitWidth(1048575))
    }
}
