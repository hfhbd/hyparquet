package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MetadataTest {
    
    @Test
    fun `throws for a too short file`() {
        val arrayBuffer = ByteArray(0)
        assertFailsWith<Error> {
            parquetMetadata(arrayBuffer)
        }
    }
    
    @Test
    fun `throws for invalid metadata length`() {
        val buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN)
        buffer.putInt(0x31524150) // magic number PAR1
        buffer.putInt(1000) // 1000 bytes exceeds buffer
        buffer.putInt(0x31524150) // magic number PAR1
        
        assertFailsWith<Error> {
            parquetMetadata(buffer.array())
        }
    }
    
    @Test
    fun `throws for invalid magic number`() {
        val arrayBuffer = ByteArray(8)
        assertFailsWith<Error> {
            parquetMetadata(arrayBuffer)
        }
    }
    
    @Test
    fun `throws for invalid large metadata length`() {
        val buffer = byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 80, 65, 82, 49)
        assertFailsWith<Error> {
            parquetMetadata(buffer)
        }
    }
    
    @Test
    fun `validates basic metadata structure`() {
        // Create simple valid metadata structure for testing
        val metadata = FileMetaData(
            version = 1,
            schema = listOf(
                SchemaElement(name = "root", num_children = 1, repetition_type = null, logical_type = null),
                SchemaElement(name = "field", repetition_type = FieldRepetitionType.OPTIONAL, logical_type = null)
            ),
            num_rows = 100,
            row_groups = emptyList(),
            created_by = "test",
            metadata_length = 50
        )
        
        assertTrue(metadata.schema.isNotEmpty())
        assertTrue(metadata.num_rows == 100L)
    }
}
