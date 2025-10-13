package hyparquet

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.io.File

class ParquetFileValidationTest {

    @Test
    fun `validate multiple parquet files have correct magic numbers`() = runBlocking {
        val testFilesDir = File("src/test/resources/files")
        val parquetFiles = testFilesDir.listFiles { _, name -> name.endsWith(".parquet") }
            ?.take(5) // Test first 5 files
            ?: return@runBlocking

        for (file in parquetFiles) {
            println("Testing file: ${file.name}")
            
            try {
                val asyncBuffer = asyncBufferFromFile(file.absolutePath)
                assertTrue(asyncBuffer.byteLength > 8, "File ${file.name} too small")
                
                // Read last 8 bytes to check for PAR1 magic
                val lastBytes = asyncBuffer.slice(asyncBuffer.byteLength - 8)
                lastBytes.order(java.nio.ByteOrder.LITTLE_ENDIAN)
                val magic = lastBytes.getInt(lastBytes.remaining() - 4)
                
                assertEquals(0x31524150, magic, "File ${file.name} should have PAR1 magic number")
                println("✓ ${file.name}: ${asyncBuffer.byteLength} bytes, valid PAR1 magic")
                
            } catch (e: Exception) {
                println("✗ ${file.name}: Failed - ${e.message}")
                throw e
            }
        }
    }

    @Test
    fun `test JSON utilities with sample data`() {
        // Test our JSON conversion utilities work
        val sampleData = mapOf(
            "string" to "hello",
            "number" to 42,
            "boolean" to true,
            "bytes" to byteArrayOf(1, 2, 3),
            "nested" to mapOf("inner" to "value")
        )
        
        val json = toJson(sampleData)
        assertNotNull(json)
        println("JSON conversion successful: $json")
    }
}