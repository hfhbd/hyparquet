package app.softwork.hyparquet

/**
 * Read parquet data rows from a file-like object.
 * Reads the minimal number of row groups and columns to satisfy the request.
 *
 * Returns a void when complete.
 * Errors are thrown.
 * Data is returned in callbacks onComplete, onChunk, onPage, NOT the return.
 *
 * @param options read options
 */
suspend fun parquetRead(options: ParquetReadOptions) {
    // load metadata if not provided
    val metadata = options.metadata ?: parquetMetadataAsync(options.file)

    // read row groups
    val asyncGroups = parquetReadAsync(options.copy(metadata = metadata))
    val rowStart = 0
    val rowEnd = Int.MAX_VALUE
    val onChunk = options.onChunk
    val onComplete = options.onComplete

    // skip assembly if no onComplete or onChunk, but wait for reading to finish
    if (onComplete == null && onChunk == null) {
        for (group in asyncGroups) {
            for (column in group.asyncColumns) {
                column.data()
            }
        }
        return
    }

    // assemble struct columns
    val schemaTree = parquetSchema(metadata.schema)
    val assembled = asyncGroups.map { assembleAsync(it, schemaTree) }

    // onChunk emit all chunks (don't await)
    if (onChunk != null) {
        for (asyncGroup in assembled) {
            for (asyncColumn in asyncGroup.asyncColumns) {
                val columnDatas = asyncColumn.data()
                var rowStart = asyncGroup.groupStart
                for (columnData in columnDatas) {
                    onChunk(ColumnData(
                        columnName = asyncColumn.pathInSchema[0],
                        columnData = columnData,
                        rowStart = rowStart,
                        rowEnd = rowStart + getArrayLength(columnData)
                    ))
                    rowStart += getArrayLength(columnData)
                }
            }
        }
    }

    // onComplete transpose column chunks to rows
    if (onComplete != null) {
        val rows = mutableListOf<Any?>()
        for (asyncGroup in assembled) {
            // filter to rows in range
            val selectStart = maxOf(rowStart - asyncGroup.groupStart, 0)
            val selectEnd = minOf(rowEnd - asyncGroup.groupStart, asyncGroup.groupRows)
            // transpose column chunks to rows in output
            val groupData = asyncGroupToRows(asyncGroup.asyncColumns, selectStart, selectEnd, options.rowFormat)
            concat(rows, DecodedArray.AnyArrayType(groupData))
        }
        @Suppress("UNCHECKED_CAST")
        onComplete(rows as List<Any>)
    } else {
        // wait for all async groups to finish (complete takes care of this)
        for (asyncGroup in assembled) {
            for (asyncColumn in asyncGroup.asyncColumns) {
                asyncColumn.data()
            }
        }
    }
}

fun parquetReadAsync(options: ParquetReadOptions): List<AsyncRowGroup> {
    val metadata = options.metadata ?: throw Error("parquet requires metadata")

    // prefetch byte ranges
    val plan = parquetPlan(metadata)
    val file = prefetchAsyncBuffer(options.file, plan.fetches)

    // read row groups
    return plan.groups.map { groupPlan -> readRowGroup(options.copy(file = file), metadata, groupPlan) }
}

/**
 * This is a helper function to read parquet row data as a promise.
 * It is a wrapper around the more configurable parquetRead function.
 *
 * @param options
 * @returns resolves when all requested rows and columns are parsed
 */
suspend fun parquetReadObjects(options: BaseParquetReadOptions): List<Map<String, Any?>> {
    val result = mutableListOf<Any>()
    parquetRead(ParquetReadOptions(
        file = options.file,
        metadata = options.metadata,
        onChunk = options.onChunk,
        onPage = options.onPage,
        compressors = options.compressors,
        utf8 = options.utf8,
        parsers = options.parsers,
        rowFormat = RowFormat.OBJECT,
        onComplete = { rows -> result.addAll(rows) }
    ))
    @Suppress("UNCHECKED_CAST")
    return result as List<Map<String, Any?>>
}

// Helper function
private fun getArrayLength(array: DecodedArray): Int {
    return when (array) {
        is DecodedArray.ByteArrayType -> array.array.size
        is DecodedArray.IntArrayType -> array.array.size
        is DecodedArray.LongArrayType -> array.array.size
        is DecodedArray.FloatArrayType -> array.array.size
        is DecodedArray.DoubleArrayType -> array.array.size
        is DecodedArray.AnyArrayType -> array.array.size
    }
}
