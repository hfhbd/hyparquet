package app.softwork.hyparquet

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class MetadataTest {
    
    private val parquetFiles = getParquetTestFiles()
    
    @Test
    fun `parse metadata from all parquet files`() = runTest {
        for (filename in parquetFiles) {
            val asyncBuffer = asyncBufferFromFile("test/files/$filename")
            val arrayBuffer = asyncBuffer.slice(0, null)
            val result = toJson(parquetMetadata(arrayBuffer))
            val base = filename.replace(".parquet", "")
            val expectedFile = File("test/files/$base.metadata.json")
            
            if (expectedFile.exists()) {
                val expected = Json.parseToJsonElement(expectedFile.readText())
                assertEquals(expected, result, "Failed for file: $filename")
            }
        }
    }
    
    @Test
    fun `parse metadata async from all parquet files`() = runTest {
        for (filename in parquetFiles) {
            val asyncBuffer = asyncBufferFromFile("test/files/$filename")
            val result = parquetMetadataAsync(asyncBuffer)
            val base = filename.replace(".parquet", "")
            val expectedFile = File("test/files/$base.metadata.json")
            
            if (expectedFile.exists()) {
                val expected = Json.parseToJsonElement(expectedFile.readText())
                assertEquals(expected, toJson(result), "Failed for file: $filename")
            }
        }
    }
    
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
    fun `throws for invalid magic number async`() = runTest {
        val buffer = byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte())
        val asyncBuffer = object : AsyncBuffer {
            override val byteLength: Long = buffer.size.toLong()
            override suspend fun slice(start: Long, end: Long?): ByteArray {
                val actualEnd = (end ?: byteLength).toInt()
                return buffer.copyOfRange(start.toInt(), actualEnd)
            }
        }
        
        assertFailsWith<Error> {
            parquetMetadataAsync(asyncBuffer)
        }
    }
    
    @Test
    fun `throws for invalid metadata length async`() = runTest {
        val buffer = byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 80, 65, 82, 49)
        val asyncBuffer = object : AsyncBuffer {
            override val byteLength: Long = buffer.size.toLong()
            override suspend fun slice(start: Long, end: Long?): ByteArray {
                val actualEnd = (end ?: byteLength).toInt()
                return buffer.copyOfRange(start.toInt(), actualEnd)
            }
        }
        
        assertFailsWith<Error> {
            parquetMetadataAsync(asyncBuffer)
        }
    }
}
