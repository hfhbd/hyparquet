package hyparquet

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * AsyncBuffer implementation for local files using Kotlin coroutines
 */
class FileAsyncBuffer(private val file: File) : AsyncBuffer {
    override val byteLength: Int = file.length().toInt()

    override suspend fun slice(start: Int, end: Int?): ByteBuffer = withContext(Dispatchers.IO) {
        val actualEnd = end ?: byteLength
        val length = actualEnd - start
        
        RandomAccessFile(file, "r").use { raf ->
            raf.seek(start.toLong())
            val bytes = ByteArray(length)
            raf.readFully(bytes)
            ByteBuffer.wrap(bytes)
        }
    }
}

/**
 * Construct an AsyncBuffer for a local file
 */
suspend fun asyncBufferFromFile(filename: String): AsyncBuffer {
    return FileAsyncBuffer(File(filename))
}

/**
 * Read all bytes from a ByteBuffer as a ByteArray
 */
fun ByteBuffer.toByteArray(): ByteArray {
    val bytes = ByteArray(remaining())
    get(bytes)
    return bytes
}

/**
 * Extension function to convert ByteBuffer to various numeric array types
 */
fun ByteBuffer.toIntArray(): IntArray {
    val ints = IntArray(remaining() / 4)
    asIntBuffer().get(ints)
    return ints
}

fun ByteBuffer.toLongArray(): LongArray {
    val longs = LongArray(remaining() / 8)
    asLongBuffer().get(longs)
    return longs
}

fun ByteBuffer.toFloatArray(): FloatArray {
    val floats = FloatArray(remaining() / 4)
    asFloatBuffer().get(floats)
    return floats
}

fun ByteBuffer.toDoubleArray(): DoubleArray {
    val doubles = DoubleArray(remaining() / 8)
    asDoubleBuffer().get(doubles)
    return doubles
}