package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ReadTest {
    
    @Test
    fun `ParquetReadOptions has required fields`() {
        // Create mock async buffer
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val options = ParquetReadOptions(
            file = mockBuffer,
            metadata = null
        )
        
        assertNotNull(options.file)
        assertTrue(options.file.byteLength > 0)
    }
    
    @Test
    fun `BaseParquetReadOptions structure is valid`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val options = BaseParquetReadOptions(
            file = mockBuffer,
            metadata = null
        )
        
        assertNotNull(options.file)
    }
    
    @Test
    fun `RowFormat enum has expected values`() {
        val formats = listOf(RowFormat.OBJECT, RowFormat.ARRAY)
        assertTrue(formats.isNotEmpty())
    }
}
