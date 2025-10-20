package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class PackageTest {
    
    @Test
    fun `package structure is valid`() {
        // Test that core types are accessible
        val types = listOf(
            ParquetType.BOOLEAN,
            ParquetType.INT32,
            ParquetType.INT64,
            ParquetType.FLOAT,
            ParquetType.DOUBLE,
            ParquetType.BYTE_ARRAY,
            ParquetType.FIXED_LEN_BYTE_ARRAY,
            ParquetType.INT96
        )
        
        assertEquals(8, types.size)
    }
    
    @Test
    fun `compression codecs are defined`() {
        val codecs = listOf(
            CompressionCodec.UNCOMPRESSED,
            CompressionCodec.SNAPPY,
            CompressionCodec.GZIP,
            CompressionCodec.LZO,
            CompressionCodec.BROTLI,
            CompressionCodec.LZ4,
            CompressionCodec.ZSTD,
            CompressionCodec.LZ4_RAW
        )
        
        assertEquals(8, codecs.size)
    }
    
    @Test
    fun `field repetition types are defined`() {
        val types = listOf(
            FieldRepetitionType.REQUIRED,
            FieldRepetitionType.OPTIONAL,
            FieldRepetitionType.REPEATED
        )
        
        assertEquals(3, types.size)
    }
    
    @Test
    fun `encoding types are defined`() {
        assertNotNull(Encoding.PLAIN)
        assertNotNull(Encoding.RLE)
        assertNotNull(Encoding.PLAIN_DICTIONARY)
        assertNotNull(Encoding.RLE_DICTIONARY)
    }
}
