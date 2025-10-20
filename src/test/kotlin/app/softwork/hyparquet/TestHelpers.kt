package app.softwork.hyparquet

import kotlinx.serialization.json.Json
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Read file and parse as JSON
 */
fun fileToJson(filePath: String): Any? {
    val content = File(filePath).readText()
    return Json.parseToJsonElement(content)
}

/**
 * Make a DataReader from bytes
 */
fun reader(bytes: ByteArray): DataReader {
    return DataReader(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN), 0)
}

/**
 * Get all .parquet files from test/files directory
 */
fun getParquetTestFiles(): List<String> {
    val testFilesDir = File("test/files")
    return testFilesDir.listFiles { _, name -> name.endsWith(".parquet") }
        ?.map { it.name }
        ?.sorted()
        ?: emptyList()
}

/**
 * Compressors for testing - stub implementations for now
 * In a real implementation, you would use actual compression libraries
 */
val testCompressors: Compressors = mapOf(
    CompressionCodec.UNCOMPRESSED to { input, _ -> input },
    CompressionCodec.SNAPPY to { input, outputLength ->
        val output = ByteArray(outputLength)
        snappyUncompress(input, output)
        output
    },
    // Add other compressors as needed
    CompressionCodec.GZIP to { input, _ ->
        // Placeholder - would need actual GZIP implementation
        input
    },
    CompressionCodec.BROTLI to { input, _ ->
        // Placeholder - would need actual Brotli implementation
        input
    },
    CompressionCodec.LZ4 to { input, _ ->
        // Placeholder - would need actual LZ4 implementation
        input
    },
    CompressionCodec.ZSTD to { input, _ ->
        // Placeholder - would need actual ZSTD implementation
        input
    },
    CompressionCodec.LZ4_RAW to { input, _ ->
        // Placeholder - would need actual LZ4_RAW implementation
        input
    }
)
