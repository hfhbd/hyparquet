package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ColumnTest {
    
    @Test
    fun `ColumnDecoder has correct properties`() {
        val element = SchemaElement(
            name = "test_column",
            type = ParquetType.INT32,
            repetition_type = FieldRepetitionType.OPTIONAL,
            logical_type = null
        )
        
        val decoder = ColumnDecoder(
            columnName = "test_column",
            type = ParquetType.INT32,
            element = element,
            codec = CompressionCodec.UNCOMPRESSED,
            parsers = DEFAULT_PARSERS
        )
        
        assertEquals("test_column", decoder.columnName)
        assertEquals(ParquetType.INT32, decoder.type)
        assertEquals(CompressionCodec.UNCOMPRESSED, decoder.codec)
        assertNotNull(decoder.parsers)
    }
    
    @Test
    fun `RowGroupSelect has correct structure`() {
        val select = RowGroupSelect(
            groupStart = 0,
            selectStart = 10,
            selectEnd = 50,
            groupRows = 100
        )
        
        assertEquals(0, select.groupStart)
        assertEquals(10, select.selectStart)
        assertEquals(50, select.selectEnd)
        assertEquals(100, select.groupRows)
    }
    
    @Test
    fun `AsyncColumn data function is callable`() {
        val column = AsyncColumn(
            pathInSchema = listOf("test"),
            data = suspend { emptyList() }
        )
        
        assertEquals(listOf("test"), column.pathInSchema)
        assertNotNull(column.data)
    }
}
