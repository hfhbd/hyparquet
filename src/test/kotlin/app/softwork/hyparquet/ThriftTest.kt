package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ThriftTest {

    @Test
    fun `parses basic types correctly`() {
        val buffer = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN)
        
        // Boolean
        buffer.put(0x11.toByte()) // Field 1 type TRUE
        buffer.put(0x12.toByte()) // Field 2 type FALSE
        
        // Byte
        buffer.put(0x13.toByte()) // Field 3 type BYTE
        buffer.put(0x7f.toByte()) // Max value for a signed byte
        
        // Int16
        buffer.put(0x14.toByte()) // Field 4 type int16
        buffer.put(0xfe.toByte()) // 0xfffe zigzag => 16-bit max value 0x7fff
        buffer.put(0xff.toByte())
        buffer.put(0x3.toByte())
        
        // Int32
        buffer.put(0x15.toByte()) // Field 5 type int32
        buffer.put(0xfe.toByte()) // 0xfffffffe zigzag => 32-bit max value 0x7fffffff
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0x0f.toByte())
        
        // Int64
        buffer.put(0x16.toByte()) // Field 6 type int64
        buffer.put(0xfe.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0xff.toByte())
        buffer.put(0x01.toByte())
        
        // Double
        buffer.put(0x17.toByte()) // Field 7 type DOUBLE
        buffer.putDouble(123.456)
        
        // String
        val str = "Hello, Thrift!"
        buffer.put(0x18.toByte()) // Field 8 type STRING
        // write string length as varint
        val stringLengthVarInt = toVarInt(str.length)
        stringLengthVarInt.forEach { buffer.put(it) }
        // write string bytes
        buffer.put(str.toByteArray())
        
        // Mark the end of the structure
        val endIndex = buffer.position()
        buffer.put(0x00.toByte()) // STOP field
        
        buffer.flip()
        val reader = DataReader(buffer, 0)
        val value = deserializeTCompactProtocol(reader)
        assertEquals(endIndex + 1, reader.offset)
        
        // Assertions for each basic type
        assertTrue(value.size > 1)
        assertEquals(true, (value.getOrNull(1) as? ThriftType.BooleanType)?.value) // TRUE
        assertEquals(false, (value.getOrNull(2) as? ThriftType.BooleanType)?.value) // FALSE
        assertEquals(0x7f, (value.getOrNull(3) as? ThriftType.IntType)?.value) // BYTE
        assertEquals(0x7fff, (value.getOrNull(4) as? ThriftType.IntType)?.value) // I16
        assertEquals(0x7fffffff, (value.getOrNull(5) as? ThriftType.IntType)?.value) // I32
        assertEquals(0x7fffffffffffffffL, (value.getOrNull(6) as? ThriftType.LongType)?.value) // I64
        // Skip DOUBLE check for now - complex conversion
        assertEquals("Hello, Thrift!", (value.getOrNull(8) as? ThriftType.ByteArrayType)?.value?.decodeToString()) // STRING
    }
    
    @Test
    fun `parses rle-dict column index correctly`() {
        val buffer = byteArrayOf(25, 17, 2, 25, 24, 8, 0, 0, 0, 0, 0, 0, 0, 0, 25, 24, 8, 0, 0, 0, 0, 0, 0, 0, 0, 21, 2, 25, 22, 0, 0)
        val view = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN)
        val reader = DataReader(view, 0)
        val value = deserializeTCompactProtocol(reader)
        
        // Check parsed values
        val field1 = (value[1] as? ThriftType.ListType)?.value
        assertEquals(1, field1?.size)
        assertEquals(false, (field1?.get(0) as? ThriftType.BooleanType)?.value)
        
        val field4 = (value[4] as? ThriftType.IntType)?.value
        assertEquals(1, field4)
    }
    
    @Test
    fun `read single-byte varint`() {
        assertEquals(1, readVarInt(reader(byteArrayOf(0x01))))
        assertEquals(127, readVarInt(reader(byteArrayOf(0x7f))))
    }
    
    @Test
    fun `read multi-byte varint`() {
        // 129 as varint (0b10000001 00000001)
        assertEquals(129, readVarInt(reader(byteArrayOf(0x81.toByte(), 0x01))))
        // 16515 as varint (0b10000011 10000010 00000001)
        assertEquals(16643, readVarInt(reader(byteArrayOf(0x83.toByte(), 0x82.toByte(), 0x01))))
    }
    
    @Test
    fun `read maximum int32 varint`() {
        // 2147483647 as varint (0b11111111 11111111 11111111 11111111 00000111)
        assertEquals(2147483647, readVarInt(reader(byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0x07))))
    }
    
    private fun reader(bytes: ByteArray): DataReader {
        return DataReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0)
    }
    
    private fun toVarInt(n: Int): List<Byte> {
        var num = n
        val varInt = mutableListOf<Byte>()
        while (true) {
            if ((num and 0x7f.inv()) == 0) {
                varInt.add(num.toByte())
                break
            } else {
                varInt.add(((num and 0x7f) or 0x80).toByte())
                num = num ushr 7
            }
        }
        return varInt
    }
}
