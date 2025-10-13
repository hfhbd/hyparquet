package hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder

const val DEFAULT_INITIAL_FETCH_SIZE = 1 shl 19 // 512kb

/**
 * Read parquet metadata from an async buffer.
 */
suspend fun parquetMetadataAsync(
    asyncBuffer: AsyncBuffer,
    options: MetadataOptions = MetadataOptions(),
    initialFetchSize: Int = DEFAULT_INITIAL_FETCH_SIZE
): FileMetaData {
    if (asyncBuffer.byteLength < 0) {
        throw Error("parquet expected AsyncBuffer")
    }

    // fetch last bytes (footer) of the file
    val footerOffset = maxOf(0, asyncBuffer.byteLength - initialFetchSize)
    val footerBuffer = asyncBuffer.slice(footerOffset)
    
    // Validate footer magic number "PAR1"
    val footerView = footerBuffer.order(ByteOrder.LITTLE_ENDIAN)
    if (footerView.getInt(footerBuffer.remaining() - 4) != 0x31524150) {
        throw Error("parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLength = footerView.getInt(footerBuffer.remaining() - 8)
    if (metadataLength > asyncBuffer.byteLength - 8) {
        throw Error("parquet metadata length $metadataLength exceeds available buffer ${asyncBuffer.byteLength - 8}")
    }

    // check if metadata size fits inside the initial fetch
    return if (metadataLength + 8 > initialFetchSize) {
        // fetch the rest of the metadata
        val metadataOffset = asyncBuffer.byteLength - metadataLength - 8
        val metadataBuffer = asyncBuffer.slice(metadataOffset, footerOffset)
        // combine initial fetch with the new slice
        val combinedBuffer = ByteBuffer.allocate(metadataLength + 8)
        combinedBuffer.put(metadataBuffer)
        val footerSlice = footerBuffer.duplicate()
        footerSlice.position(footerOffset - metadataOffset)
        combinedBuffer.put(footerSlice)
        combinedBuffer.flip()
        parquetMetadata(combinedBuffer, options)
    } else {
        // parse metadata from the footer
        parquetMetadata(footerBuffer, options)
    }
}

/**
 * Read parquet metadata from a buffer synchronously.
 */
fun parquetMetadata(buffer: ByteBuffer, options: MetadataOptions = MetadataOptions()): FileMetaData {
    val view = buffer.order(ByteOrder.LITTLE_ENDIAN)
    
    // Use default parsers if not given
    val parsers = options.parsers ?: DefaultParsers

    // Validate footer magic number "PAR1"
    if (view.remaining() < 8) {
        throw Error("parquet file is too short")
    }
    if (view.getInt(view.remaining() - 4) != 0x31524150) {
        throw Error("parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLengthOffset = view.remaining() - 8
    val metadataLength = view.getInt(metadataLengthOffset)
    val availableMetadataLength = view.remaining() - 8
    if (metadataLength > availableMetadataLength) {
        throw Error("parquet metadata length $metadataLength exceeds available buffer $availableMetadataLength")
    }

    // Metadata starts from the end minus length minus 8 bytes for footer
    val metadataOffset = view.remaining() - metadataLength - 8
    view.position(metadataOffset)
    
    // Create a reader for the metadata
    val reader = DataReader(view, metadataOffset)
    
    // Parse thrift metadata
    val thriftMetadata = deserializeTCompactProtocol(reader)
    
    // Convert thrift metadata to our types
    return convertThriftFileMetaData(thriftMetadata, parsers)
}

/**
 * Convert thrift metadata to FileMetaData
 */
private fun convertThriftFileMetaData(thrift: Map<String, ThriftType>, parsers: ParquetParsers): FileMetaData {
    fun getInt(key: String): Int = when (val value = thrift["field_$key"]) {
        is ThriftType.NumberType -> value.value.toInt()
        else -> throw Error("Expected int for field $key")
    }
    
    fun getLong(key: String): Long = when (val value = thrift["field_$key"]) {
        is ThriftType.BigIntType -> value.value
        is ThriftType.NumberType -> value.value.toLong()
        else -> throw Error("Expected long for field $key")
    }
    
    fun getString(key: String): String? = when (val value = thrift["field_$key"]) {
        is ThriftType.ByteArrayType -> String(value.value, Charsets.UTF_8)
        null -> null
        else -> throw Error("Expected string for field $key")
    }
    
    fun getList(key: String): List<ThriftType> = when (val value = thrift["field_$key"]) {
        is ThriftType.ListType -> value.value
        null -> emptyList()
        else -> throw Error("Expected list for field $key")
    }

    val version = getInt("1")
    val schema = getList("2").map { convertThriftSchemaElement(it, parsers) }
    val numRows = getLong("3")
    val rowGroups = getList("4").map { convertThriftRowGroup(it, parsers) }
    val keyValueMetadata = thrift["field_5"]?.let { 
        when (it) {
            is ThriftType.ListType -> it.value.map { convertThriftKeyValue(it) }
            else -> null
        }
    }
    val createdBy = getString("6")
    
    return FileMetaData(
        version = version,
        schema = schema,
        num_rows = numRows,
        row_groups = rowGroups,
        key_value_metadata = keyValueMetadata,
        created_by = createdBy,
        metadata_length = 0 // This will be filled in later
    )
}

/**
 * Convert thrift schema element
 */
private fun convertThriftSchemaElement(thrift: ThriftType, parsers: ParquetParsers): SchemaElement {
    when (thrift) {
        is ThriftType.ObjectType -> {
            val obj = thrift.value
            
            fun getInt(key: String): Int? = when (val value = obj["field_$key"]) {
                is ThriftType.NumberType -> value.value.toInt()
                else -> null
            }
            
            fun getString(key: String): String = when (val value = obj["field_$key"]) {
                is ThriftType.ByteArrayType -> String(value.value, Charsets.UTF_8)
                else -> throw Error("Expected string for field $key")
            }
            
            val type = getInt("1")?.let { ParquetType.values()[it] }
            val typeLength = getInt("2")
            val repetitionType = getInt("3")?.let { FieldRepetitionType.values()[it] }
            val name = getString("4")
            val numChildren = getInt("5")
            val convertedType = getInt("6")?.let { ConvertedType.values()[it] }
            val scale = getInt("7")
            val precision = getInt("8")
            val fieldId = getInt("9")
            
            return SchemaElement(
                type = type,
                type_length = typeLength,
                repetition_type = repetitionType,
                name = name,
                num_children = numChildren,
                converted_type = convertedType,
                scale = scale,
                precision = precision,
                field_id = fieldId,
                logical_type = null // TODO: implement logical type conversion
            )
        }
        else -> throw Error("Expected object for schema element")
    }
}

/**
 * Convert thrift row group
 */
private fun convertThriftRowGroup(thrift: ThriftType, parsers: ParquetParsers): RowGroup {
    when (thrift) {
        is ThriftType.ObjectType -> {
            val obj = thrift.value
            
            fun getList(key: String): List<ThriftType> = when (val value = obj["field_$key"]) {
                is ThriftType.ListType -> value.value
                else -> emptyList()
            }
            
            fun getLong(key: String): Long = when (val value = obj["field_$key"]) {
                is ThriftType.BigIntType -> value.value
                is ThriftType.NumberType -> value.value.toLong()
                else -> throw Error("Expected long for field $key")
            }
            
            val columns = getList("1").map { convertThriftColumnChunk(it, parsers) }
            val totalByteSize = getLong("2")
            val numRows = getLong("3")
            
            return RowGroup(
                columns = columns,
                total_byte_size = totalByteSize,
                num_rows = numRows,
                sorting_columns = null,
                file_offset = null,
                total_compressed_size = null,
                ordinal = null
            )
        }
        else -> throw Error("Expected object for row group")
    }
}

/**
 * Convert thrift column chunk
 */
private fun convertThriftColumnChunk(thrift: ThriftType, parsers: ParquetParsers): ColumnChunk {
    when (thrift) {
        is ThriftType.ObjectType -> {
            val obj = thrift.value
            
            fun getLong(key: String): Long = when (val value = obj["field_$key"]) {
                is ThriftType.BigIntType -> value.value
                is ThriftType.NumberType -> value.value.toLong()
                else -> throw Error("Expected long for field $key")
            }
            
            val fileOffset = getLong("2")
            val metaData = obj["field_3"]?.let { convertThriftColumnMetaData(it, parsers) }
            
            return ColumnChunk(
                file_path = null,
                file_offset = fileOffset,
                meta_data = metaData,
                offset_index_offset = null,
                offset_index_length = null,
                column_index_offset = null,
                column_index_length = null,
                crypto_metadata = null,
                encrypted_column_metadata = null
            )
        }
        else -> throw Error("Expected object for column chunk")
    }
}

/**
 * Convert thrift column metadata
 */
private fun convertThriftColumnMetaData(thrift: ThriftType, parsers: ParquetParsers): ColumnMetaData {
    when (thrift) {
        is ThriftType.ObjectType -> {
            val obj = thrift.value
            
            fun getInt(key: String): Int = when (val value = obj["field_$key"]) {
                is ThriftType.NumberType -> value.value.toInt()
                else -> throw Error("Expected int for field $key")
            }
            
            fun getLong(key: String): Long = when (val value = obj["field_$key"]) {
                is ThriftType.BigIntType -> value.value
                is ThriftType.NumberType -> value.value.toLong()
                else -> throw Error("Expected long for field $key")
            }
            
            fun getList(key: String): List<ThriftType> = when (val value = obj["field_$key"]) {
                is ThriftType.ListType -> value.value
                else -> emptyList()
            }
            
            val type = ParquetType.values()[getInt("1")]
            val encodings = getList("2").map { 
                when (it) {
                    is ThriftType.NumberType -> Encoding.values()[it.value.toInt()]
                    else -> throw Error("Expected encoding number")
                }
            }
            val pathInSchema = getList("3").map { 
                when (it) {
                    is ThriftType.ByteArrayType -> String(it.value, Charsets.UTF_8)
                    else -> throw Error("Expected string for path")
                }
            }
            val codec = CompressionCodec.values()[getInt("4")]
            val numValues = getLong("5")
            val totalUncompressedSize = getLong("6")
            val totalCompressedSize = getLong("7")
            val dataPageOffset = getLong("9")
            
            return ColumnMetaData(
                type = type,
                encodings = encodings,
                path_in_schema = pathInSchema,
                codec = codec,
                num_values = numValues,
                total_uncompressed_size = totalUncompressedSize,
                total_compressed_size = totalCompressedSize,
                key_value_metadata = null,
                data_page_offset = dataPageOffset,
                index_page_offset = null,
                dictionary_page_offset = null,
                statistics = null,
                encoding_stats = null,
                bloom_filter_offset = null,
                bloom_filter_length = null,
                size_statistics = null
            )
        }
        else -> throw Error("Expected object for column metadata")
    }
}

/**
 * Convert thrift key value
 */
private fun convertThriftKeyValue(thrift: ThriftType): KeyValue {
    when (thrift) {
        is ThriftType.ObjectType -> {
            val obj = thrift.value
            
            fun getString(key: String): String? = when (val value = obj["field_$key"]) {
                is ThriftType.ByteArrayType -> String(value.value, Charsets.UTF_8)
                else -> null
            }
            
            val key = getString("1") ?: throw Error("Key value must have key")
            val value = getString("2")
            
            return KeyValue(key = key, value = value)
        }
        else -> throw Error("Expected object for key value")
    }
}