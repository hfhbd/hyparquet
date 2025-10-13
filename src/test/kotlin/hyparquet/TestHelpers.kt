package hyparquet

import kotlinx.serialization.json.Json
import java.io.File
import java.nio.ByteBuffer

/**
 * Read file and parse as JSON
 */
fun fileToJson(filePath: String): Any? {
    val file = File(filePath)
    val content = file.readText()
    return Json.parseToJsonElement(content)
}

/**
 * Make a DataReader from bytes
 */
fun reader(bytes: IntArray): DataReader {
    val byteArray = bytes.map { it.toByte() }.toByteArray()
    return DataReader(ByteBuffer.wrap(byteArray), 0)
}

/**
 * Wraps an AsyncBuffer to count the number of fetches made
 */
class CountingBuffer(private val asyncBuffer: AsyncBuffer) : AsyncBuffer {
    var fetches: Int = 0
        private set
    var bytes: Int = 0
        private set

    override val byteLength: Int get() = asyncBuffer.byteLength

    override suspend fun slice(start: Int, end: Int?): ByteBuffer {
        fetches++
        bytes += (end ?: asyncBuffer.byteLength) - start
        return asyncBuffer.slice(start, end)
    }
}

/**
 * Basic compressors implementation - for now just placeholder
 */
val compressors: Compressors = mapOf(
    CompressionCodec.UNCOMPRESSED to { input: ByteArray, _: Int -> input },
    CompressionCodec.SNAPPY to { input: ByteArray, outputLength: Int ->
        // TODO: Implement snappy decompression
        throw NotImplementedError("Snappy decompression not yet implemented")
    },
    CompressionCodec.GZIP to { input: ByteArray, outputLength: Int ->
        // TODO: Implement gzip decompression
        throw NotImplementedError("GZIP decompression not yet implemented")
    },
    CompressionCodec.LZO to { _: ByteArray, _: Int ->
        throw NotImplementedError("LZO is not supported")
    },
    CompressionCodec.BROTLI to { input: ByteArray, outputLength: Int ->
        // TODO: Implement brotli decompression
        throw NotImplementedError("Brotli decompression not yet implemented")
    },
    CompressionCodec.LZ4 to { input: ByteArray, outputLength: Int ->
        // TODO: Implement LZ4 decompression
        throw NotImplementedError("LZ4 decompression not yet implemented")
    },
    CompressionCodec.ZSTD to { input: ByteArray, outputLength: Int ->
        // TODO: Implement ZSTD decompression
        throw NotImplementedError("ZSTD decompression not yet implemented")
    },
    CompressionCodec.LZ4_RAW to { input: ByteArray, outputLength: Int ->
        // TODO: Implement LZ4_RAW decompression
        throw NotImplementedError("LZ4_RAW decompression not yet implemented")
    }
)