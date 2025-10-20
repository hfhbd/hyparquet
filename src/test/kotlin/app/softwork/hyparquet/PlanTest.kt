package app.softwork.hyparquet

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PlanTest {
    
    @Test
    fun `generates a basic query plan`() {
        // Create simple metadata for testing
        val metadata = FileMetaData(
            version = 1,
            schema = listOf(
                SchemaElement(name = "root", num_children = 1, repetition_type = null, logical_type = null),
                SchemaElement(name = "field", repetition_type = FieldRepetitionType.OPTIONAL, type = ParquetType.INT32, logical_type = null)
            ),
            num_rows = 100,
            row_groups = listOf(
                RowGroup(
                    columns = listOf(
                        ColumnChunk(
                            file_offset = 4,
                            meta_data = ColumnMetaData(
                                type = ParquetType.INT32,
                                path_in_schema = listOf("field"),
                                codec = CompressionCodec.UNCOMPRESSED,
                                num_values = 100,
                                total_uncompressed_size = 400,
                                total_compressed_size = 400,
                                data_page_offset = 4
                            )
                        )
                    ),
                    total_byte_size = 400,
                    num_rows = 100
                )
            ),
            created_by = "test",
            metadata_length = 100
        )
        
        val plan = parquetPlan(metadata)
        
        assertNotNull(plan)
        assertEquals(metadata, plan.metadata)
        assertTrue(plan.fetches.isNotEmpty())
        assertTrue(plan.groups.isNotEmpty())
        assertEquals(100, plan.groups[0].groupRows)
        assertEquals(0, plan.groups[0].groupStart)
    }
    
    @Test
    fun `getColumnRange calculates correct range`() {
        val columnMetaData = ColumnMetaData(
            type = ParquetType.INT32,
            path_in_schema = listOf("field"),
            codec = CompressionCodec.UNCOMPRESSED,
            num_values = 100,
            total_uncompressed_size = 400,
            total_compressed_size = 400,
            data_page_offset = 100
        )
        
        val range = getColumnRange(columnMetaData)
        
        assertEquals(100, range.startByte)
        assertEquals(500, range.endByte)
    }
    
    @Test
    fun `getColumnRange uses dictionary offset if present`() {
        val columnMetaData = ColumnMetaData(
            type = ParquetType.INT32,
            path_in_schema = listOf("field"),
            codec = CompressionCodec.UNCOMPRESSED,
            num_values = 100,
            total_uncompressed_size = 400,
            total_compressed_size = 400,
            data_page_offset = 200,
            dictionary_page_offset = 100
        )
        
        val range = getColumnRange(columnMetaData)
        
        assertEquals(100, range.startByte)
        assertEquals(500, range.endByte)
    }
}
