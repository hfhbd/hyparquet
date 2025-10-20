package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ReadFilesTest {
    
    @Test
    fun `read options support row format array`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val options = ParquetReadOptions(
            file = mockBuffer,
            rowFormat = RowFormat.ARRAY
        )
        
        assertNotNull(options.rowFormat)
        assertTrue(options.rowFormat == RowFormat.ARRAY)
    }
    
    @Test
    fun `read options support row format object`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val options = ParquetReadOptions(
            file = mockBuffer,
            rowFormat = RowFormat.OBJECT
        )
        
        assertNotNull(options.rowFormat)
        assertTrue(options.rowFormat == RowFormat.OBJECT)
    }
    
    @Test
    fun `parquet read options support callbacks`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        var onCompleteCalled = false
        var onChunkCalled = false
        
        val options = ParquetReadOptions(
            file = mockBuffer,
            onComplete = { _ -> onCompleteCalled = true },
            onChunk = { _ -> onChunkCalled = true }
        )
        
        assertNotNull(options.onComplete)
        assertNotNull(options.onChunk)
    }
    
    @Test
    fun `compressors can be provided in options`() {
        val mockBuffer = object : AsyncBuffer {
            override val byteLength: Long = 1000
            override suspend fun slice(start: Long, end: Long?): ByteArray = byteArrayOf()
        }
        
        val mockCompressors: Compressors = mapOf(
            CompressionCodec.SNAPPY to { input, _ -> input }
        )
        
        val options = BaseParquetReadOptions(
            file = mockBuffer,
            compressors = mockCompressors
        )
        
        assertNotNull(options.compressors)
        assertTrue(options.compressors!!.containsKey(CompressionCodec.SNAPPY))
    }
}
