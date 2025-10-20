package app.softwork.hyparquet

// Combine column chunks into a single byte range if less than 32mb
private const val columnChunkAggregation = 1 shl 25 // 32mb

/**
 * Plan which byte ranges to read to satisfy a read request.
 * Metadata must be non-null.
 */
fun parquetPlan(metadata: FileMetaData): QueryPlan {
    val rowStart = 0
    val groups = mutableListOf<GroupPlan>()
    val fetches = mutableListOf<ByteRange>()

    // find which row groups to read
    var groupStart = 0 // first row index of the current group
    for (rowGroup in metadata.row_groups) {
        val groupRows = rowGroup.num_rows.toInt()
        val groupEnd = groupStart + groupRows
        // if row group overlaps with row range, add it to the plan
        if (groupRows > 0 && groupEnd >= rowStart) {
            val ranges = mutableListOf<ByteRange>()
            // loop through each column chunk
            for (column in rowGroup.columns) {
                if (column.file_path != null) throw Error("parquet file_path not supported")
                if (column.meta_data == null) throw Error("parquet column metadata is undefined")
                // add included columns to the plan
                ranges.add(getColumnRange(column.meta_data))
            }
            val selectStart = maxOf(rowStart - groupStart, 0)
            val selectEnd = minOf(Int.MAX_VALUE - groupStart, groupRows)
            groups.add(GroupPlan(ranges, rowGroup, groupStart, groupRows, selectStart, selectEnd))

            // map group plan to ranges
            val groupSize = ranges.last().endByte - ranges.first().startByte
            if (groupSize < columnChunkAggregation) {
                // full row group
                fetches.add(ByteRange(
                    startByte = ranges.first().startByte,
                    endByte = ranges.last().endByte
                ))
            } else if (ranges.isNotEmpty()) {
                fetches.addAll(ranges)
            }
        }

        groupStart = groupEnd
    }

    return QueryPlan(metadata, fetches, groups)
}

fun getColumnRange(columnMetaData: ColumnMetaData): ByteRange {
    val columnOffset = columnMetaData.dictionary_page_offset ?: columnMetaData.data_page_offset
    return ByteRange(
        startByte = columnOffset,
        endByte = columnOffset + columnMetaData.total_compressed_size
    )
}

/**
 * Prefetch byte ranges from an AsyncBuffer.
 */
fun prefetchAsyncBuffer(file: AsyncBuffer, fetches: List<ByteRange>): AsyncBuffer {
    // fetch byte ranges from the file
    val promises = fetches.map { (startByte, endByte) ->
        suspend { file.slice(startByte, endByte) }
    }
    
    return object : AsyncBuffer {
        override val byteLength: Long = file.byteLength

        override suspend fun slice(start: Long, end: Long?): ByteArray {
            val actualEnd = end ?: file.byteLength
            // find matching slice
            val index = fetches.indexOfFirst { (startByte, endByte) ->
                startByte <= start && actualEnd <= endByte
            }
            if (index < 0) throw Error("no prefetch for range [$start, $actualEnd]")
            
            val fetch = fetches[index]
            if (fetch.startByte != start || fetch.endByte != actualEnd) {
                // slice a subrange of the prefetch
                val startOffset = (start - fetch.startByte).toInt()
                val endOffset = (actualEnd - fetch.startByte).toInt()
                val buffer = promises[index]()
                return buffer.sliceArray(startOffset until endOffset)
            } else {
                return promises[index]()
            }
        }
    }
}
