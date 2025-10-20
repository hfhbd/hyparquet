package app.softwork.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.math.max

const val defaultInitialFetchSize = 1 shl 19 // 512kb

/**
 * Read parquet metadata from an async buffer.
 */
suspend fun parquetMetadataAsync(
    asyncBuffer: AsyncBuffer,
    initialFetchSize: Int = defaultInitialFetchSize
): FileMetaData {
    if (asyncBuffer.byteLength < 0) throw Error("parquet expected AsyncBuffer")

    // fetch last bytes (footer) of the file
    val footerOffset = max(0, asyncBuffer.byteLength - initialFetchSize)
    val footerBuffer = asyncBuffer.slice(footerOffset, asyncBuffer.byteLength)

    // Check for parquet magic number "PAR1"
    val footerView = ByteBuffer.wrap(footerBuffer).order(ByteOrder.LITTLE_ENDIAN)
    if (footerView.getInt(footerBuffer.size - 4) != 0x31524150) {
        throw Error("parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLength = footerView.getInt(footerBuffer.size - 8)
    if (metadataLength > asyncBuffer.byteLength - 8) {
        throw Error("parquet metadata length $metadataLength exceeds available buffer ${asyncBuffer.byteLength - 8}")
    }

    // check if metadata size fits inside the initial fetch
    return if (metadataLength + 8 > initialFetchSize) {
        // fetch the rest of the metadata
        val metadataOffset = asyncBuffer.byteLength - metadataLength - 8
        val metadataBuffer = asyncBuffer.slice(metadataOffset, footerOffset)
        // combine initial fetch with the new slice
        val combinedBuffer = ByteArray(metadataLength + 8)
        System.arraycopy(metadataBuffer, 0, combinedBuffer, 0, metadataBuffer.size)
        System.arraycopy(footerBuffer, 0, combinedBuffer, (footerOffset - metadataOffset).toInt(), footerBuffer.size)
        parquetMetadata(combinedBuffer)
    } else {
        // parse metadata from the footer
        parquetMetadata(footerBuffer)
    }
}

/**
 * Read parquet metadata from a buffer synchronously.
 *
 * @param arrayBuffer parquet file footer
 * @returns parquet metadata object
 */
fun parquetMetadata(arrayBuffer: ByteArray): FileMetaData {
    val view = ByteBuffer.wrap(arrayBuffer).order(ByteOrder.LITTLE_ENDIAN)

    // Validate footer magic number "PAR1"
    if (view.limit() < 8) {
        throw Error("parquet file is too short")
    }
    if (view.getInt(view.limit() - 4) != 0x31524150) {
        throw Error("parquet file invalid (footer != PAR1)")
    }

    // Parquet files store metadata at the end of the file
    // Metadata length is 4 bytes before the last PAR1
    val metadataLengthOffset = view.limit() - 8
    val metadata_length = view.getInt(metadataLengthOffset)
    if (metadata_length > view.limit() - 8) {
        throw Error("parquet metadata length $metadata_length exceeds available buffer ${view.limit() - 8}")
    }

    val metadataOffset = metadataLengthOffset - metadata_length
    val reader = DataReader(view, metadataOffset)
    val metadata = deserializeTCompactProtocol(reader)

    // Parse metadata from thrift data
    val version = (metadata[1] as? ThriftType.IntType)?.value ?: 0
    val schemaList = metadata[2] as? ThriftType.ListType
    val schema = schemaList?.value?.mapNotNull { field ->
        (field as? ThriftType.ObjectType)?.let { parseSchemaElement(it.value) }
    } ?: emptyList()
    
    val num_rows = (metadata[3] as? ThriftType.LongType)?.value ?: 0L
    val rowGroupsList = metadata[4] as? ThriftType.ListType
    val row_groups = rowGroupsList?.value?.mapNotNull { rowGroup ->
        (rowGroup as? ThriftType.ObjectType)?.let { parseRowGroup(it.value) }
    } ?: emptyList()
    
    val createdByArray = (metadata[6] as? ThriftType.ByteArrayType)?.value
    val created_by = createdByArray?.decodeToString()

    return FileMetaData(
        version = version,
        schema = schema,
        num_rows = num_rows,
        row_groups = row_groups,
        created_by = created_by,
        metadata_length = metadata_length
    )
}

private fun parseSchemaElement(field: ThriftObject): SchemaElement {
    val type = (field.getOrNull(1) as? ThriftType.IntType)?.value
    val type_length = (field.getOrNull(2) as? ThriftType.IntType)?.value
    val repetition_type = (field.getOrNull(3) as? ThriftType.IntType)?.value?.let { 
        FieldRepetitionType.values()[it]
    }
    val name = (field.getOrNull(4) as? ThriftType.ByteArrayType)?.value?.decodeToString() ?: ""
    val num_children = (field.getOrNull(5) as? ThriftType.IntType)?.value
    val converted_type = (field.getOrNull(6) as? ThriftType.IntType)?.value?.let { 
        ConvertedType.values()[it]
    }
    val scale = (field.getOrNull(7) as? ThriftType.IntType)?.value
    val precision = (field.getOrNull(8) as? ThriftType.IntType)?.value
    val logical_type = (field.getOrNull(10) as? ThriftType.ObjectType)?.let { 
        parseLogicalType(it.value)
    }

    return SchemaElement(
        type = type?.let { ParquetType.values().getOrNull(it) },
        type_length = type_length,
        repetition_type = repetition_type,
        name = name,
        num_children = num_children,
        converted_type = converted_type,
        scale = scale,
        precision = precision,
        logical_type = logical_type
    )
}

private fun parseRowGroup(rowGroupField: ThriftObject): RowGroup {
    val columnsList = rowGroupField.getOrNull(1) as? ThriftType.ListType
    val columns = columnsList?.value?.mapNotNull { column ->
        (column as? ThriftType.ObjectType)?.let { parseColumnChunk(it.value) }
    } ?: emptyList()
    
    val total_byte_size = (rowGroupField.getOrNull(2) as? ThriftType.LongType)?.value ?: 0L
    val num_rows = (rowGroupField.getOrNull(3) as? ThriftType.LongType)?.value ?: 0L
    val file_offset = (rowGroupField.getOrNull(5) as? ThriftType.LongType)?.value
    val total_compressed_size = (rowGroupField.getOrNull(6) as? ThriftType.LongType)?.value

    return RowGroup(columns, total_byte_size, num_rows, file_offset, total_compressed_size)
}

private fun parseColumnChunk(columnField: ThriftObject): ColumnChunk {
    val filePath = (columnField.getOrNull(1) as? ThriftType.ByteArrayType)?.value?.decodeToString()
    val file_offset = (columnField.getOrNull(2) as? ThriftType.LongType)?.value ?: 0L
    val meta_data = (columnField.getOrNull(3) as? ThriftType.ObjectType)?.let { parseColumnMetaData(it.value) }
    val offset_index_offset = (columnField.getOrNull(4) as? ThriftType.LongType)?.value
    val offset_index_length = (columnField.getOrNull(5) as? ThriftType.IntType)?.value
    val column_index_offset = (columnField.getOrNull(6) as? ThriftType.LongType)?.value
    val column_index_length = (columnField.getOrNull(7) as? ThriftType.IntType)?.value

    return ColumnChunk(filePath, file_offset, meta_data, offset_index_offset, offset_index_length, column_index_offset, column_index_length)
}

private fun parseColumnMetaData(columnField3: ThriftObject): ColumnMetaData {
    val type = (columnField3.getOrNull(1) as? ThriftType.IntType)?.value?.let { ParquetType.values()[it] } ?: ParquetType.BOOLEAN
    val path_in_schema = (columnField3.getOrNull(3) as? ThriftType.ListType)?.value?.mapNotNull {
        (it as? ThriftType.ByteArrayType)?.value?.decodeToString()
    } ?: emptyList()
    val codec = (columnField3.getOrNull(4) as? ThriftType.IntType)?.value?.let { CompressionCodec.values()[it] } ?: CompressionCodec.UNCOMPRESSED
    val num_values = (columnField3.getOrNull(5) as? ThriftType.LongType)?.value ?: 0L
    val total_uncompressed_size = (columnField3.getOrNull(6) as? ThriftType.LongType)?.value ?: 0L
    val total_compressed_size = (columnField3.getOrNull(7) as? ThriftType.LongType)?.value ?: 0L
    val data_page_offset = (columnField3.getOrNull(9) as? ThriftType.LongType)?.value ?: 0L
    val index_page_offset = (columnField3.getOrNull(10) as? ThriftType.LongType)?.value
    val dictionary_page_offset = (columnField3.getOrNull(11) as? ThriftType.LongType)?.value

    return ColumnMetaData(type, path_in_schema, codec, num_values, total_uncompressed_size, total_compressed_size, data_page_offset, index_page_offset, dictionary_page_offset)
}

/**
 * Return a tree of schema elements from parquet metadata.
 */
fun parquetSchema(schema: List<SchemaElement>): SchemaTree {
    return getSchemaPath(schema, emptyList())[0]
}

private fun parseLogicalType(logicalType: ThriftObject): LogicalType? {
    if (logicalType.getOrNull(1) != null) return LogicalType.STRING
    if (logicalType.getOrNull(2) != null) return LogicalType.MAP
    if (logicalType.getOrNull(3) != null) return LogicalType.LIST
    if (logicalType.getOrNull(4) != null) return LogicalType.ENUM
    if (logicalType.getOrNull(5) != null) {
        val decimal = (logicalType[5] as? ThriftType.ObjectType)?.value
        val scale = (decimal?.getOrNull(1) as? ThriftType.IntType)?.value ?: 0
        val precision = (decimal?.getOrNull(2) as? ThriftType.IntType)?.value ?: 0
        return LogicalType.DECIMAL(precision, scale)
    }
    if (logicalType.getOrNull(6) != null) return LogicalType.DATE
    if (logicalType.getOrNull(7) != null) {
        val time = (logicalType[7] as? ThriftType.ObjectType)?.value
        val isAdjustedToUTC = (time?.getOrNull(1) as? ThriftType.BooleanType)?.value ?: false
        val unit = (time?.getOrNull(2) as? ThriftType.ObjectType)?.value?.let { parseTimeUnit(it) } ?: TimeUnit.MILLIS
        return LogicalType.TIME(isAdjustedToUTC, unit)
    }
    if (logicalType.getOrNull(8) != null) {
        val timestamp = (logicalType[8] as? ThriftType.ObjectType)?.value
        val isAdjustedToUTC = (timestamp?.getOrNull(1) as? ThriftType.BooleanType)?.value ?: false
        val unit = (timestamp?.getOrNull(2) as? ThriftType.ObjectType)?.value?.let { parseTimeUnit(it) } ?: TimeUnit.MILLIS
        return LogicalType.TIMESTAMP(isAdjustedToUTC, unit)
    }
    if (logicalType.getOrNull(10) != null) {
        val integer = (logicalType[10] as? ThriftType.ObjectType)?.value
        val bitWidth = (integer?.getOrNull(1) as? ThriftType.IntType)?.value ?: 32
        val isSigned = (integer?.getOrNull(2) as? ThriftType.BooleanType)?.value ?: true
        return LogicalType.INTEGER(bitWidth, isSigned)
    }
    if (logicalType.getOrNull(11) != null) return LogicalType.NULL
    if (logicalType.getOrNull(12) != null) return LogicalType.JSON
    if (logicalType.getOrNull(13) != null) return LogicalType.BSON
    if (logicalType.getOrNull(14) != null) return LogicalType.UUID
    if (logicalType.getOrNull(15) != null) return LogicalType.FLOAT16
    if (logicalType.getOrNull(16) != null) return LogicalType.VARIANT
    throw Error("unsupported logical type")
}

private fun parseTimeUnit(unit: ThriftObject): TimeUnit {
    if (unit.getOrNull(1) != null) return TimeUnit.MILLIS
    if (unit.getOrNull(2) != null) return TimeUnit.MICROS
    if (unit.getOrNull(3) != null) return TimeUnit.NANOS
    throw Error("parquet time unit required")
}
