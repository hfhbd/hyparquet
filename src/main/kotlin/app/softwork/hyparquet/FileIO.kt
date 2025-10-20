package app.softwork.hyparquet

import java.io.RandomAccessFile
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Construct an AsyncBuffer for a local file using Java NIO.
 */
suspend fun asyncBufferFromFile(filename: String): AsyncBuffer {
    val path = Paths.get(filename)
    val size = Files.size(path)
    
    return object : AsyncBuffer {
        override val byteLength: Long = size
        
        override suspend fun slice(start: Long, end: Long?): ByteArray {
            val actualEnd = end ?: size
            val length = (actualEnd - start).toInt()
            val buffer = ByteArray(length)
            
            RandomAccessFile(filename, "r").use { raf ->
                raf.seek(start)
                raf.readFully(buffer)
            }
            
            return buffer
        }
    }
}
