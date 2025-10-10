package io.github.hfhbd.hyparquet

/**
 * Main entry point for the Hyparquet Kotlin library.
 * 
 * This object provides convenient access to the main parquet reading functionality.
 */
object Hyparquet {
    
    /**
     * Read parquet data from a file and return rows as objects (maps).
     * 
     * @param file The parquet file as AsyncBuffer
     * @param columns Optional list of columns to read (all columns if null)
     * @param rowStart First row index to read (inclusive, default 0)
     * @param rowEnd Last row index to read (exclusive, default reads all rows)
     * @param parsers Custom parsers for advanced types (default parsers if null)
     * @return List of rows where each row is a Map<String, Any>
     */
    suspend fun readObjects(
        file: AsyncBuffer,
        columns: List<String>? = null,
        rowStart: Int = 0,
        rowEnd: Int? = null,
        parsers: ParquetParsers = DefaultParsers
    ): List<Map<String, Any>> {
        return parquetReadObjects(file, columns, rowStart, rowEnd, parsers)
    }
    
    /**
     * Read parquet data from a file and return rows as arrays.
     * 
     * @param file The parquet file as AsyncBuffer
     * @param columns Optional list of columns to read (all columns if null)
     * @param rowStart First row index to read (inclusive, default 0)
     * @param rowEnd Last row index to read (exclusive, default reads all rows)
     * @param parsers Custom parsers for advanced types (default parsers if null)
     * @return List of rows where each row is a List<Any>
     */
    suspend fun readArrays(
        file: AsyncBuffer,
        columns: List<String>? = null,
        rowStart: Int = 0,
        rowEnd: Int? = null,
        parsers: ParquetParsers = DefaultParsers
    ): List<List<Any>> {
        return parquetReadArrays(file, columns, rowStart, rowEnd, parsers)
    }
    
    /**
     * Read parquet metadata from a file.
     * 
     * @param file The parquet file as AsyncBuffer
     * @param parsers Custom parsers for advanced types (default parsers if null)
     * @return FileMetaData containing the parquet file metadata
     */
    suspend fun readMetadata(
        file: AsyncBuffer,
        parsers: ParquetParsers = DefaultParsers
    ): FileMetaData {
        return parquetMetadataAsync(file, parsers)
    }
    
    /**
     * Create an AsyncBuffer from a byte array.
     * 
     * @param data The byte array containing parquet data
     * @return AsyncBuffer that can be used with read functions
     */
    fun fromByteArray(data: ByteArray): AsyncBuffer {
        return ByteArrayAsyncBuffer(data)
    }
    
    /**
     * Create an AsyncBuffer from a URL.
     * 
     * @param url The URL pointing to a parquet file
     * @param headers Optional HTTP headers to include in requests
     * @return AsyncBuffer that can be used with read functions
     */
    suspend fun fromUrl(
        url: String,
        headers: Map<String, String> = emptyMap()
    ): AsyncBuffer {
        return asyncBufferFromUrl(url, headers = headers)
    }
}