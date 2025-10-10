package io.github.hfhbd.hyparquet

import kotlinx.serialization.Serializable
import java.util.*

/**
 * File-like object that can read slices of a file asynchronously.
 */
interface AsyncBuffer {
    val byteLength: Int
    suspend fun slice(start: Int, end: Int? = null): ByteArray
}

interface DataReader {
    val view: ByteArray
    var offset: Int
}

/**
 * Custom parsers for columns
 */
interface ParquetParsers {
    fun timestampFromMilliseconds(millis: Long): Any
    fun timestampFromMicroseconds(micros: Long): Any  
    fun timestampFromNanoseconds(nanos: Long): Any
    fun dateFromDays(days: Int): Any
    fun stringFromBytes(bytes: ByteArray): Any
}

/**
 * Parquet Metadata options for metadata parsing
 */
data class MetadataOptions(
    val parsers: ParquetParsers? = null
)

/**
 * Parquet query options for reading data
 */
data class BaseParquetReadOptions(
    val file: AsyncBuffer, // file-like object containing parquet data
    val metadata: FileMetaData? = null, // parquet metadata, will be parsed if not provided
    val columns: List<String>? = null, // columns to read, all columns if undefined
    val rowStart: Int? = null, // first requested row index (inclusive)
    val rowEnd: Int? = null, // last requested row index (exclusive)
    val onChunk: ((ColumnData) -> Unit)? = null, // called when a column chunk is parsed
    val onPage: ((ColumnData) -> Unit)? = null, // called when a data page is parsed
    val compressors: Compressors? = null, // custom decompressors
    val utf8: Boolean = true, // decode byte arrays as utf8 strings (default true)
    val parsers: ParquetParsers? = null // custom parsers to decode advanced types
)

/**
 * A run of column data
 */
data class ColumnData(
    val columnName: String,
    val columnData: List<Any>,
    val rowStart: Int,
    val rowEnd: Int // exclusive
)

// Parquet type enums
enum class ParquetType {
    BOOLEAN,
    INT32,
    INT64,
    INT96, // deprecated
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY
}

enum class FieldRepetitionType {
    REQUIRED,
    OPTIONAL,
    REPEATED
}

enum class ConvertedType {
    UTF8,
    MAP,
    MAP_KEY_VALUE,
    LIST,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL
}

enum class TimeUnit {
    MILLIS,
    MICROS,
    NANOS
}

enum class EdgeInterpolationAlgorithm {
    SPHERICAL,
    VINCENTY,
    THOMAS,
    ANDOYER,
    KARNEY
}

// Logical type sealed class hierarchy
@Serializable
sealed class LogicalType {
    @Serializable
    object STRING : LogicalType()
    
    @Serializable
    object MAP : LogicalType()
    
    @Serializable
    object LIST : LogicalType()
    
    @Serializable
    object ENUM : LogicalType()
    
    @Serializable
    object DATE : LogicalType()
    
    @Serializable
    object INTERVAL : LogicalType()
    
    @Serializable
    object NULL : LogicalType()
    
    @Serializable
    object JSON : LogicalType()
    
    @Serializable
    object BSON : LogicalType()
    
    @Serializable
    object UUID : LogicalType()
    
    @Serializable
    object FLOAT16 : LogicalType()
    
    @Serializable
    object VARIANT : LogicalType()
    
    @Serializable
    data class DECIMAL(val precision: Int, val scale: Int) : LogicalType()
    
    @Serializable
    data class TIME(val isAdjustedToUTC: Boolean, val unit: TimeUnit) : LogicalType()
    
    @Serializable
    data class TIMESTAMP(val isAdjustedToUTC: Boolean, val unit: TimeUnit) : LogicalType()
    
    @Serializable
    data class INTEGER(val bitWidth: Int, val isSigned: Boolean) : LogicalType()
    
    @Serializable
    data class GEOMETRY(val crs: String? = null) : LogicalType()
    
    @Serializable
    data class GEOGRAPHY(val crs: String? = null, val algorithm: EdgeInterpolationAlgorithm? = null) : LogicalType()
}

enum class Encoding {
    PLAIN,
    GROUP_VAR_INT, // deprecated
    PLAIN_DICTIONARY,
    RLE,
    BIT_PACKED, // deprecated
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY,
    RLE_DICTIONARY,
    BYTE_STREAM_SPLIT
}

enum class CompressionCodec {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
    LZ4_RAW
}

typealias Compressors = Map<CompressionCodec, (ByteArray, Int) -> ByteArray>

enum class PageType {
    DATA_PAGE,
    INDEX_PAGE,
    DICTIONARY_PAGE,
    DATA_PAGE_V2
}

data class KeyValue(
    val key: String,
    val value: String? = null
)

// Parquet file metadata types
@Serializable
data class FileMetaData(
    val version: Int,
    val schema: List<SchemaElement>,
    val numRows: Long,
    val rowGroups: List<RowGroup>,
    val keyValueMetadata: List<KeyValue>? = null,
    val createdBy: String? = null,
    val metadataLength: Int
)

@Serializable
data class SchemaTree(
    val children: List<SchemaTree>,
    val count: Int,
    val element: SchemaElement,
    val path: List<String>
)

@Serializable
data class SchemaElement(
    val type: ParquetType? = null,
    val typeLength: Int? = null,
    val repetitionType: FieldRepetitionType? = null,
    val name: String,
    val numChildren: Int? = null,
    val convertedType: ConvertedType? = null,
    val scale: Int? = null,
    val precision: Int? = null,
    val fieldId: Int? = null,
    val logicalType: LogicalType? = null
)

@Serializable
data class RowGroup(
    val columns: List<ColumnChunk>,
    val totalByteSize: Long,
    val numRows: Long,
    val sortingColumns: List<SortingColumn>? = null,
    val fileOffset: Long? = null,
    val totalCompressedSize: Long? = null,
    val ordinal: Int? = null
)

@Serializable
data class ColumnChunk(
    val filePath: String? = null,
    val fileOffset: Long,
    val metaData: ColumnMetaData? = null,
    val offsetIndexOffset: Long? = null,
    val offsetIndexLength: Int? = null,
    val columnIndexOffset: Long? = null,
    val columnIndexLength: Int? = null,
    val encryptedColumnMetadata: ByteArray? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ColumnChunk

        if (filePath != other.filePath) return false
        if (fileOffset != other.fileOffset) return false
        if (metaData != other.metaData) return false
        if (offsetIndexOffset != other.offsetIndexOffset) return false
        if (offsetIndexLength != other.offsetIndexLength) return false
        if (columnIndexOffset != other.columnIndexOffset) return false
        if (columnIndexLength != other.columnIndexLength) return false
        if (encryptedColumnMetadata != null) {
            if (other.encryptedColumnMetadata == null) return false
            if (!encryptedColumnMetadata.contentEquals(other.encryptedColumnMetadata)) return false
        } else if (other.encryptedColumnMetadata != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = filePath?.hashCode() ?: 0
        result = 31 * result + fileOffset.hashCode()
        result = 31 * result + (metaData?.hashCode() ?: 0)
        result = 31 * result + (offsetIndexOffset?.hashCode() ?: 0)
        result = 31 * result + (offsetIndexLength ?: 0)
        result = 31 * result + (columnIndexOffset?.hashCode() ?: 0)
        result = 31 * result + (columnIndexLength ?: 0)
        result = 31 * result + (encryptedColumnMetadata?.contentHashCode() ?: 0)
        return result
    }
}

@Serializable
data class ColumnMetaData(
    val type: ParquetType,
    val encodings: List<Encoding>,
    val pathInSchema: List<String>,
    val codec: CompressionCodec,
    val numValues: Long,
    val totalUncompressedSize: Long,
    val totalCompressedSize: Long,
    val keyValueMetadata: List<KeyValue>? = null,
    val dataPageOffset: Long,
    val indexPageOffset: Long? = null,
    val dictionaryPageOffset: Long? = null,
    val statistics: Statistics? = null,
    val encodingStats: List<PageEncodingStats>? = null,
    val bloomFilterOffset: Long? = null,
    val bloomFilterLength: Int? = null,
    val sizeStatistics: SizeStatistics? = null
)

typealias MinMaxType = Any // bigint | boolean | number | string | Date | ByteArray

@Serializable
data class Statistics(
    val max: MinMaxType? = null,
    val min: MinMaxType? = null,
    val nullCount: Long? = null,
    val distinctCount: Long? = null,
    val maxValue: MinMaxType? = null,
    val minValue: MinMaxType? = null,
    val isMaxValueExact: Boolean? = null,
    val isMinValueExact: Boolean? = null
)

@Serializable
data class SizeStatistics(
    val unencodedByteArrayDataBytes: Long? = null,
    val repetitionLevelHistogram: List<Long>? = null,
    val definitionLevelHistogram: List<Long>? = null
)

@Serializable
data class PageEncodingStats(
    val pageType: PageType,
    val encoding: Encoding,
    val count: Int
)

@Serializable
data class SortingColumn(
    val columnIdx: Int,
    val descending: Boolean,
    val nullsFirst: Boolean
)