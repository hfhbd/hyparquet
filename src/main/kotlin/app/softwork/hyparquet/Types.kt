package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Custom parsers for columns
 */
interface ParquetParsers {
    fun timestampFromMilliseconds(millis: Long): Any?
    fun timestampFromMicroseconds(micros: Long): Any?
    fun timestampFromNanoseconds(nanos: Long): Any?
    fun dateFromDays(days: Int): Any?
    fun stringFromBytes(bytes: ByteArray): String
}

/**
 * Parquet query options for reading data
 */
data class BaseParquetReadOptions(
    val file: AsyncBuffer, // file-like object containing parquet data
    val metadata: FileMetaData? = null, // parquet metadata, will be parsed if not provided
    val onChunk: ((ColumnData) -> Unit)? = null, // called when a column chunk is parsed
    val onPage: ((ColumnData) -> Unit)? = null, // called when a data page is parsed
    val compressors: Compressors? = null, // custom decompressors
    val utf8: Boolean = true, // decode byte arrays as utf8 strings (default true)
    val parsers: ParquetParsers? = null // custom parsers to decode advanced types
)

enum class RowFormat {
    ARRAY, OBJECT
}

/**
 * Parquet query options for reading data
 */
data class ParquetReadOptions(
    val file: AsyncBuffer,
    val metadata: FileMetaData? = null,
    val onChunk: ((ColumnData) -> Unit)? = null,
    val onPage: ((ColumnData) -> Unit)? = null,
    val compressors: Compressors? = null,
    val utf8: Boolean = true,
    val parsers: ParquetParsers? = null,
    val rowFormat: RowFormat = RowFormat.ARRAY,
    val onComplete: ((List<Any>) -> Unit)? = null
)

/**
 * A run of column data
 */
data class ColumnData(
    val columnName: String,
    val columnData: DecodedArray,
    val rowStart: Int,
    val rowEnd: Int // exclusive
)

/**
 * File-like object that can read slices of a file asynchronously.
 */
interface AsyncBuffer {
    val byteLength: Long
    suspend fun slice(start: Long, end: Long?): ByteArray
}

data class ByteRange(
    val startByte: Long,
    val endByte: Long // exclusive
)

data class DataReader(
    val view: ByteBuffer,
    var offset: Int = 0
) {
    init {
        view.order(ByteOrder.LITTLE_ENDIAN)
    }
}

// Parquet file metadata types
data class FileMetaData(
    val version: Int,
    val schema: List<SchemaElement>,
    val num_rows: Long,
    val row_groups: List<RowGroup>,
    val created_by: String?,
    val metadata_length: Int
)

data class SchemaTree(
    val children: List<SchemaTree>,
    val count: Int,
    val element: SchemaElement,
    val path: List<String>
)

data class SchemaElement(
    val type: ParquetType? = null,
    val type_length: Int? = null,
    val repetition_type: FieldRepetitionType? = null,
    val name: String,
    val num_children: Int? = null,
    val converted_type: ConvertedType? = null,
    val scale: Int? = null,
    val precision: Int? = null,
    val logical_type: LogicalType? = null
)

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
    MILLIS, MICROS, NANOS
}

sealed class LogicalType {
    data object STRING : LogicalType()
    data object MAP : LogicalType()
    data object LIST : LogicalType()
    data object ENUM : LogicalType()
    data object DATE : LogicalType()
    data object INTERVAL : LogicalType()
    data object NULL : LogicalType()
    data object JSON : LogicalType()
    data object BSON : LogicalType()
    data object UUID : LogicalType()
    data object FLOAT16 : LogicalType()
    data object VARIANT : LogicalType()
    data class DECIMAL(val precision: Int, val scale: Int) : LogicalType()
    data class TIME(val isAdjustedToUTC: Boolean, val unit: TimeUnit) : LogicalType()
    data class TIMESTAMP(val isAdjustedToUTC: Boolean, val unit: TimeUnit) : LogicalType()
    data class INTEGER(val bitWidth: Int, val isSigned: Boolean) : LogicalType()
}

data class RowGroup(
    val columns: List<ColumnChunk>,
    val total_byte_size: Long,
    val num_rows: Long,
    val file_offset: Long? = null,
    val total_compressed_size: Long? = null
)

data class ColumnChunk(
    val file_path: String? = null,
    val file_offset: Long,
    val meta_data: ColumnMetaData? = null,
    val offset_index_offset: Long? = null,
    val offset_index_length: Int? = null,
    val column_index_offset: Long? = null,
    val column_index_length: Int? = null
)

data class ColumnMetaData(
    val type: ParquetType,
    val path_in_schema: List<String>,
    val codec: CompressionCodec,
    val num_values: Long,
    val total_uncompressed_size: Long,
    val total_compressed_size: Long,
    val data_page_offset: Long,
    val index_page_offset: Long? = null,
    val dictionary_page_offset: Long? = null
)

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

// Parquet file header types
data class PageHeader(
    val type: PageType,
    val uncompressed_page_size: Int,
    val compressed_page_size: Int,
    val data_page_header: DataPageHeader? = null,
    val dictionary_page_header: DictionaryPageHeader? = null,
    val data_page_header_v2: DataPageHeaderV2? = null
)

data class DataPageHeader(
    val num_values: Int,
    val encoding: Encoding
)

data class DictionaryPageHeader(
    val num_values: Int
)

data class DataPageHeaderV2(
    val num_values: Int,
    val num_nulls: Int,
    val num_rows: Int,
    val encoding: Encoding,
    val definition_levels_byte_length: Int,
    val repetition_levels_byte_length: Int,
    val is_compressed: Boolean? = null
)

data class DataPage(
    val definitionLevels: IntArray? = null,
    val repetitionLevels: IntArray,
    val dataPage: DecodedArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DataPage) return false

        if (definitionLevels != null) {
            if (other.definitionLevels == null) return false
            if (!definitionLevels.contentEquals(other.definitionLevels)) return false
        } else if (other.definitionLevels != null) return false

        if (!repetitionLevels.contentEquals(other.repetitionLevels)) return false
        if (dataPage != other.dataPage) return false

        return true
    }

    override fun hashCode(): Int {
        var result = definitionLevels?.contentHashCode() ?: 0
        result = 31 * result + repetitionLevels.contentHashCode()
        result = 31 * result + dataPage.hashCode()
        return result
    }
}

// DecodedArray can be one of several array types
sealed class DecodedArray {
    data class ByteArrayType(val array: ByteArray) : DecodedArray() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ByteArrayType) return false
            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int = array.contentHashCode()
    }

    data class IntArrayType(val array: IntArray) : DecodedArray() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is IntArrayType) return false
            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int = array.contentHashCode()
    }

    data class LongArrayType(val array: LongArray) : DecodedArray() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is LongArrayType) return false
            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int = array.contentHashCode()
    }

    data class FloatArrayType(val array: FloatArray) : DecodedArray() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is FloatArrayType) return false
            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int = array.contentHashCode()
    }

    data class DoubleArrayType(val array: DoubleArray) : DecodedArray() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is DoubleArrayType) return false
            return array.contentEquals(other.array)
        }

        override fun hashCode(): Int = array.contentHashCode()
    }

    data class AnyArrayType(val array: List<Any?>) : DecodedArray()
}

typealias ThriftObject = List<ThriftType?>
sealed class ThriftType {
    data class BooleanType(val value: Boolean) : ThriftType()
    data class IntType(val value: Int) : ThriftType()
    data class LongType(val value: Long) : ThriftType()
    data class ByteArrayType(val value: ByteArray) : ThriftType() {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ByteArrayType) return false
            return value.contentEquals(other.value)
        }

        override fun hashCode(): Int = value.contentHashCode()
    }

    data class ListType(val value: List<ThriftType>) : ThriftType()
    data class ObjectType(val value: ThriftObject) : ThriftType()
}

/**
 * Query plan for which byte ranges to read.
 */
data class QueryPlan(
    val metadata: FileMetaData,
    val fetches: List<ByteRange>, // byte ranges to fetch
    val groups: List<GroupPlan> // byte ranges by row group
)

// Plan for one group
data class GroupPlan(
    val ranges: List<ByteRange>,
    val rowGroup: RowGroup, // row group metadata
    val groupStart: Int, // row index of the first row in the group
    val selectStart: Int, // row index in the group to start reading
    val selectEnd: Int, // row index in the group to stop reading
    val groupRows: Int
)

data class ColumnDecoder(
    val columnName: String? = null,
    val type: ParquetType? = null,
    val element: SchemaElement,
    val schemaPath: List<SchemaTree>? = null,
    val codec: CompressionCodec? = null,
    val parsers: ParquetParsers? = null,
    var compressors: Compressors? = null,
    var utf8: Boolean? = null
)

data class RowGroupSelect(
    val groupStart: Int, // row index of the first row in the group
    val selectStart: Int, // row index in the group to start reading
    val selectEnd: Int, // row index in the group to stop reading
    val groupRows: Int
)

data class AsyncColumn(
    val pathInSchema: List<String>,
    val data: suspend () -> List<DecodedArray>
)

data class AsyncRowGroup(
    val groupStart: Int,
    val groupRows: Int,
    val asyncColumns: List<AsyncColumn>
)
