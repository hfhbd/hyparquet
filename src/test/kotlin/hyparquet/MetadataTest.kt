package hyparquet

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.io.File

class MetadataTest {

    @Test
    fun `test basic file reading`() = runBlocking {
        // Find a simple parquet file from our test resources
        val testFile = File("src/test/resources/files/boolean_rle.parquet")
        if (!testFile.exists()) {
            println("Test file not found: ${testFile.absolutePath}")
            return@runBlocking
        }

        try {
            val asyncBuffer = asyncBufferFromFile(testFile.absolutePath)
            
            // Basic assertions
            assertTrue(asyncBuffer.byteLength > 0)
            println("File size: ${asyncBuffer.byteLength} bytes")
            
            // Try to read the last 8 bytes (should contain PAR1 magic)
            val lastBytes = asyncBuffer.slice(asyncBuffer.byteLength - 8)
            println("Last 8 bytes read successfully, remaining: ${lastBytes.remaining()}")
            
            // Check for PAR1 magic number
            lastBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN)
            val magic = lastBytes.getInt(lastBytes.remaining() - 4)
            println("Magic number: 0x${magic.toString(16).padStart(8, '0')}")
            assertEquals(0x31524150, magic, "Should have PAR1 magic number")
            
        } catch (e: Exception) {
            println("Error during basic file reading: ${e.message}")
            e.printStackTrace()
            throw e
        }
    }

    @Test
    fun `test thrift parsing basic types`() {
        // Test basic thrift functionality with STOP only
        val buffer = java.nio.ByteBuffer.wrap(byteArrayOf(0x00)) // STOP type
        val reader = DataReader(buffer, 0)
        
        // Check bounds before parsing
        if (reader.offset < reader.view.limit()) {
            val result = deserializeTCompactProtocol(reader)
            // Should be empty since we only have STOP
            assertTrue(result.isEmpty())
        } else {
            println("Buffer too small for thrift parsing")
        }
    }
}