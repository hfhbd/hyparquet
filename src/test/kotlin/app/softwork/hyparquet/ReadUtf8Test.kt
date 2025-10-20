package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ReadUtf8Test {
    
    @Test
    fun `utf8 conversion options are supported`() {
        // Test that utf8 parameter is available in read options
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val optionsWithUtf8True = BaseParquetReadOptions(
            file = mockBuffer,
            utf8 = true
        )
        
        val optionsWithUtf8False = BaseParquetReadOptions(
            file = mockBuffer,
            utf8 = false
        )
        
        assertEquals(true, optionsWithUtf8True.utf8)
        assertEquals(false, optionsWithUtf8False.utf8)
    }
    
    @Test
    fun `default utf8 behavior is true`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val optionsDefault = BaseParquetReadOptions(
            file = mockBuffer
        )
        
        // Default should be true or null (which means true)
        assertTrue(optionsDefault.utf8 == null || optionsDefault.utf8 == true)
    }
    
    @Test
    fun `column decoder respects utf8 setting`() {
        val element = SchemaElement(
            name = "test",
            type = ParquetType.BYTE_ARRAY,
            logical_type = null
        )
        
        val decoderWithUtf8 = ColumnDecoder(
            element = element,
            utf8 = true,
            parsers = DEFAULT_PARSERS
        )
        
        val decoderWithoutUtf8 = ColumnDecoder(
            element = element,
            utf8 = false,
            parsers = DEFAULT_PARSERS
        )
        
        assertEquals(true, decoderWithUtf8.utf8)
        assertEquals(false, decoderWithoutUtf8.utf8)
    }
}
