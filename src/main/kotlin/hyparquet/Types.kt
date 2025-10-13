package hyparquet

import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.*

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
interface BaseParquetReadOptions {
    val file: AsyncBuffer // file-like object containing parquet data
    val metadata: FileMetaData? // parquet metadata, will be parsed if not provided
    val columns: List<String>? // columns to read, all columns if undefined
    val rowStart: Int? // first requested row index (inclusive)
    val rowEnd: Int? // last requested row index (exclusive)
    val onChunk: ((chunk: ColumnData) -> Unit)? // called when a column chunk is parsed
    val onPage: ((chunk: ColumnData) -> Unit)? // called when a data page is parsed
    val compressors: Compressors? // custom decompressors
    val utf8: Boolean? // decode byte arrays as utf8 strings (default true)
    val parsers: ParquetParsers? // custom parsers to decode advanced types
}

sealed interface RowFormat {
    data class Array(
        val onComplete: ((rows: List<List<Any?>>) -> Unit)? = null
    ) : RowFormat
    
    data class Object(
        val onComplete: ((rows: List<Map<String, Any?>>) -> Unit)? = null
    ) : RowFormat
}

data class ParquetReadOptions(
    override val file: AsyncBuffer,
    override val metadata: FileMetaData? = null,
    override val columns: List<String>? = null,
    override val rowStart: Int? = null,
    override val rowEnd: Int? = null,
    override val onChunk: ((chunk: ColumnData) -> Unit)? = null,
    override val onPage: ((chunk: ColumnData) -> Unit)? = null,
    override val compressors: Compressors? = null,
    override val utf8: Boolean? = null,
    override val parsers: ParquetParsers? = null,
    val rowFormat: RowFormat = RowFormat.Array()
) : BaseParquetReadOptions

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
    val byteLength: Int
    suspend fun slice(start: Int, end: Int? = null): ByteBuffer
}

data class ByteRange(
    val startByte: Int,
    val endByte: Int // exclusive
)

data class DataReader(
    val view: ByteBuffer,
    val offset: Int
)

// Parquet file metadata types
@Serializable
data class FileMetaData(
    val version: Int,
    val schema: List<SchemaElement>,
    val num_rows: Long,
    val row_groups: List<RowGroup>,
    val key_value_metadata: List<KeyValue>? = null,
    val created_by: String? = null,
    val metadata_length: Int
)

data class SchemaTree(
    val children: List<SchemaTree>,
    val count: Int,
    val element: SchemaElement,
    val path: List<String>
)

@Serializable
data class SchemaElement(
    val type: ParquetType? = null,
    val type_length: Int? = null,
    val repetition_type: FieldRepetitionType? = null,
    val name: String,
    val num_children: Int? = null,
    val converted_type: ConvertedType? = null,
    val scale: Int? = null,
    val precision: Int? = null,
    val field_id: Int? = null,
    val logical_type: LogicalType? = null
)

@Serializable
enum class ParquetType {
    BOOLEAN,
    INT32,
    INT64,
    INT96, // deprecated
    FLOAT,
    DOUBLE,
    BYTE_ARRAY,
    FIXED_LEN_BYTE_ARRAY,
}

@Serializable
enum class FieldRepetitionType {
    REQUIRED,
    OPTIONAL,
    REPEATED
}

@Serializable
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

@Serializable
enum class TimeUnit { MILLIS, MICROS, NANOS }

@Serializable
data class LogicalType(
    val type: String,
    val isAdjustedToUTC: Boolean? = null,
    val unit: TimeUnit? = null,
    val precision: Int? = null,
    val scale: Int? = null
)

@Serializable
data class RowGroup(
    val columns: List<ColumnChunk>,
    val total_byte_size: Long,
    val num_rows: Long,
    val sorting_columns: List<SortingColumn>? = null,
    val file_offset: Long? = null,
    val total_compressed_size: Long? = null,
    val ordinal: Int? = null
)

@Serializable
data class ColumnChunk(
    val file_path: String? = null,
    val file_offset: Long,
    val meta_data: ColumnMetaData? = null,
    val offset_index_offset: Long? = null,
    val offset_index_length: Int? = null,
    val column_index_offset: Long? = null,
    val column_index_length: Int? = null,
    val crypto_metadata: ColumnCryptoMetaData? = null,
    val encrypted_column_metadata: ByteArray? = null
)

@Serializable
data class ColumnMetaData(
    val type: ParquetType,
    val encodings: List<Encoding>,
    val path_in_schema: List<String>,
    val codec: CompressionCodec,
    val num_values: Long,
    val total_uncompressed_size: Long,
    val total_compressed_size: Long,
    val key_value_metadata: List<KeyValue>? = null,
    val data_page_offset: Long,
    val index_page_offset: Long? = null,
    val dictionary_page_offset: Long? = null,
    val statistics: Statistics? = null,
    val encoding_stats: List<PageEncodingStats>? = null,
    val bloom_filter_offset: Long? = null,
    val bloom_filter_length: Int? = null,
    val size_statistics: SizeStatistics? = null
)

// Empty placeholder for ColumnCryptoMetaData
@Serializable
class ColumnCryptoMetaData

@Serializable
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

@Serializable
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

typealias Compressors = Map<CompressionCodec, (input: ByteArray, outputLength: Int) -> ByteArray>

@Serializable
data class KeyValue(
    val key: String,
    val value: String? = null
)

// Union type for min/max values
sealed class MinMaxType {
    data class BigIntValue(val value: Long) : MinMaxType()
    data class BooleanValue(val value: Boolean) : MinMaxType()
    data class NumberValue(val value: Double) : MinMaxType()
    data class StringValue(val value: String) : MinMaxType()
    data class DateValue(val value: Date) : MinMaxType()
    data class ByteArrayValue(val value: ByteArray) : MinMaxType()
}

@Serializable
data class Statistics(
    val max: String? = null, // Serialized as string, converted to MinMaxType
    val min: String? = null, // Serialized as string, converted to MinMaxType
    val null_count: Long? = null,
    val distinct_count: Long? = null,
    val max_value: String? = null, // Serialized as string, converted to MinMaxType
    val min_value: String? = null, // Serialized as string, converted to MinMaxType
    val is_max_value_exact: Boolean? = null,
    val is_min_value_exact: Boolean? = null
)

@Serializable
data class SizeStatistics(
    val unencoded_byte_array_data_bytes: Long? = null,
    val repetition_level_histogram: List<Long>? = null,
    val definition_level_histogram: List<Long>? = null
)

@Serializable
data class PageEncodingStats(
    val page_type: PageType,
    val encoding: Encoding,
    val count: Int
)

@Serializable
enum class PageType {
    DATA_PAGE,
    INDEX_PAGE,
    DICTIONARY_PAGE,
    DATA_PAGE_V2
}

@Serializable
data class SortingColumn(
    val column_idx: Int,
    val descending: Boolean,
    val nulls_first: Boolean
)

// Parquet file header types
@Serializable
data class PageHeader(
    val type: PageType,
    val uncompressed_page_size: Int,
    val compressed_page_size: Int,
    val crc: Int? = null,
    val data_page_header: DataPageHeader? = null,
    val index_page_header: IndexPageHeader? = null,
    val dictionary_page_header: DictionaryPageHeader? = null,
    val data_page_header_v2: DataPageHeaderV2? = null
)

@Serializable
data class DataPageHeader(
    val num_values: Int,
    val encoding: Encoding,
    val definition_level_encoding: Encoding,
    val repetition_level_encoding: Encoding,
    val statistics: Statistics? = null
)

// Empty placeholder for IndexPageHeader
@Serializable
class IndexPageHeader

@Serializable
data class DictionaryPageHeader(
    val num_values: Int,
    val encoding: Encoding,
    val is_sorted: Boolean? = null
)

@Serializable
data class DataPageHeaderV2(
    val num_values: Int,
    val num_nulls: Int,
    val num_rows: Int,
    val encoding: Encoding,
    val definition_levels_byte_length: Int,
    val repetition_levels_byte_length: Int,
    val is_compressed: Boolean? = null,
    val statistics: Statistics? = null
)

data class DataPage(
    val definitionLevels: IntArray?,
    val repetitionLevels: IntArray,
    val dataPage: DecodedArray
)

// Kotlin equivalent of TypeScript's union type for arrays
sealed class DecodedArray {
    data class Uint8(val array: ByteArray) : DecodedArray()
    data class Uint32(val array: IntArray) : DecodedArray()
    data class Int32(val array: IntArray) : DecodedArray()
    data class BigInt64(val array: LongArray) : DecodedArray()
    data class BigUint64(val array: LongArray) : DecodedArray()
    data class Float32(val array: FloatArray) : DecodedArray()
    data class Float64(val array: DoubleArray) : DecodedArray()
    data class AnyList(val array: List<Any?>) : DecodedArray()
}

@Serializable
data class OffsetIndex(
    val page_locations: List<PageLocation>,
    val unencoded_byte_array_data_bytes: List<Long>? = null
)

@Serializable
data class PageLocation(
    val offset: Long,
    val compressed_page_size: Int,
    val first_row_index: Long
)

@Serializable
data class ColumnIndex(
    val null_pages: List<Boolean>,
    val min_values: List<String>, // Serialized as strings, converted to MinMaxType
    val max_values: List<String>, // Serialized as strings, converted to MinMaxType
    val boundary_order: BoundaryOrder,
    val null_counts: List<Long>? = null,
    val repetition_level_histograms: List<Long>? = null,
    val definition_level_histograms: List<Long>? = null
)

@Serializable
enum class BoundaryOrder { UNORDERED, ASCENDING, DESCENDING }

// Thrift object representation
typealias ThriftObject = Map<String, ThriftType>

sealed class ThriftType {
    data class BooleanType(val value: Boolean) : ThriftType()
    data class NumberType(val value: Double) : ThriftType()
    data class BigIntType(val value: Long) : ThriftType()
    data class ByteArrayType(val value: ByteArray) : ThriftType()
    data class ListType(val value: List<ThriftType>) : ThriftType()
    data class ObjectType(val value: ThriftObject) : ThriftType()
}

/**
 * Query plan for which byte ranges to read.
 */
data class QueryPlan(
    val metadata: FileMetaData,
    val rowStart: Int,
    val rowEnd: Int?,
    val columns: List<String>?, // columns to read
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
    val columnName: String,
    val type: ParquetType,
    val element: SchemaElement,
    val schemaPath: List<SchemaTree>,
    val codec: CompressionCodec,
    val parsers: ParquetParsers,
    val compressors: Compressors?,
    val utf8: Boolean?
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