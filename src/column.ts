import {assembleLists} from './assemble.js'
import {
  ColumnData,
  ColumnDecoder, DataPageHeader, DataPageHeaderV2,
  DataReader,
  DecodedArray, DictionaryPageHeader,
  FieldRepetitionType,
  PageHeader,
  PageType,
  RowGroupSelect
} from './types.js'
import {convert, convertWithDictionary} from './convert.js'
import {decompressPage, readDataPage, readDataPageV2} from './datapage.js'
import {readPlain} from './plain.js'
import {isFlatColumn} from './schema.js'
import {deserializeTCompactProtocol} from './thrift.js'

/**
 * Parse column data from a buffer.
 *
 * @param {DataReader} reader
 * @param {RowGroupSelect} rowGroupSelect row group selection
 * @param {ColumnDecoder} columnDecoder column decoder params
 * @param {(chunk: ColumnData) => void} [onPage] callback for each page
 * @returns {DecodedArray[]}
 */
export function readColumn(reader: DataReader, {
  groupStart,
  selectStart,
  selectEnd
}: RowGroupSelect, columnDecoder: ColumnDecoder, onPage: (chunk: ColumnData) => void =  _ => {}): DecodedArray[] {
  const { columnName, schemaPath } = columnDecoder
  const isFlat = isFlatColumn(schemaPath)
  const chunks: DecodedArray[] = []
  let dictionary: DecodedArray | undefined = undefined
  let lastChunk: DecodedArray | undefined = undefined
  let rowCount = 0

  const emitLastChunk = onPage && (() => {
    lastChunk && onPage({
      columnName,
      columnData: lastChunk,
      rowStart: groupStart + rowCount - lastChunk.length,
      rowEnd: groupStart + rowCount,
    })
  })

  while (isFlat ? rowCount < selectEnd : reader.offset < reader.view.byteLength - 1) {
    if (reader.offset >= reader.view.byteLength - 1) break // end of reader

    // read page header
    const header = parquetHeader(reader)
    if (header.type === PageType.DICTIONARY_PAGE) {
      // assert(!dictionary)
      dictionary = readPage(reader, header, columnDecoder, dictionary, undefined, 0)
      dictionary = convert(dictionary, columnDecoder)
    } else {
      const lastChunkLength = lastChunk?.length || 0
      const values = readPage(reader, header, columnDecoder, dictionary, lastChunk, selectStart - rowCount)
      if (lastChunk === values) {
        // continued from previous page
        rowCount += values.length - lastChunkLength
      } else {
        emitLastChunk?.()
        chunks.push(values)
        rowCount += values.length
        lastChunk = values
      }
    }
  }
  emitLastChunk?.()
  // assert(rowCount >= selectEnd)
  if (rowCount > selectEnd && lastChunk) {
    // truncate last chunk to row limit
    chunks[chunks.length - 1] = lastChunk.slice(0, selectEnd - (rowCount - lastChunk.length))
  }
  return chunks
}

/**
 * Read a page (data or dictionary) from a buffer.
 *
 * @param {DataReader} reader
 * @param {PageHeader} header
 * @param {ColumnDecoder} columnDecoder
 * @param {DecodedArray | undefined} dictionary
 * @param {DecodedArray | undefined} previousChunk
 * @param {number} pageStart skip this many rows in the page
 * @returns {DecodedArray}
 */
export function readPage(reader: DataReader, header: PageHeader, columnDecoder: ColumnDecoder, dictionary: DecodedArray | undefined, previousChunk: DecodedArray | undefined, pageStart: number): DecodedArray {
  const { type, element, schemaPath, codec, compressors } = columnDecoder
  // read compressed_page_size bytes
  const compressedBytes = new Uint8Array(
    reader.view.buffer, reader.view.byteOffset + reader.offset, header.compressed_page_size
  )
  reader.offset += header.compressed_page_size

  // parse page data by type
  if (header.type === PageType.DATA_PAGE.valueOf()) {
    const daph = header.data_page_header
    if (!daph) throw new Error('parquet data page header is undefined')

    // skip unnecessary non-nested pages
    if (pageStart > daph.num_values && isFlatColumn(schemaPath)) {
      return new Array(daph.num_values) // TODO: don't allocate array
    }

    const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), codec, compressors)
    const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, columnDecoder)
    // assert(!daph.statistics?.null_count || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

    // convert types, dereference dictionary, and assemble lists
    let values = convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder)
    if (repetitionLevels.length || definitionLevels?.length) {
      const output = Array.isArray(previousChunk) ? previousChunk : []
      return assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath)
    } else {
      // wrap nested flat data by depth
      for (let i = 2; i < schemaPath.length; i++) {
        if (schemaPath[i].element.repetition_type !== FieldRepetitionType.REQUIRED) {
          values = Array.from(values, e => [e])
        }
      }
      return values
    }
  } else if (header.type === PageType.DATA_PAGE_V2) {
    const daph2 = header.data_page_header_v2
    if (!daph2) throw new Error('parquet data page header v2 is undefined')

    // skip unnecessary pages
    if (pageStart > daph2.num_rows) {
      return new Array(daph2.num_values) // TODO: don't allocate array
    }

    const { definitionLevels, repetitionLevels, dataPage } =
      readDataPageV2(compressedBytes, header, columnDecoder)

    // convert types, dereference dictionary, and assemble lists
    const values = convertWithDictionary(dataPage, dictionary, daph2.encoding, columnDecoder)
    const output = Array.isArray(previousChunk) ? previousChunk : []
    return assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath)
  } else if (header.type === PageType.DICTIONARY_PAGE) {
    const diph = header.dictionary_page_header
    if (!diph) throw new Error('parquet dictionary page header is undefined')

    const page = decompressPage(
      compressedBytes, Number(header.uncompressed_page_size), codec, compressors
    )

    const reader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 }
    return readPlain(reader, type, diph.num_values, element.type_length)
  } else {
    throw new Error(`parquet unsupported page type: ${header.type}`)
  }
}

/**
 * Read parquet header from a buffer.
 */
function parquetHeader(reader: DataReader): PageHeader {
  const header = deserializeTCompactProtocol(reader)

  // Parse parquet header from thrift data
  const type: PageType = header.field_1
  const uncompressed_page_size = header.field_2
  const compressed_page_size = header.field_3
  const data_page_header: DataPageHeader = header.field_5 && {
    num_values: header.field_5.field_1,
    encoding: header.field_5.field_2,
    statistics: header.field_5.field_5 && {
      max: header.field_5.field_5.field_1,
      min: header.field_5.field_5.field_2,
      null_count: header.field_5.field_5.field_3,
      distinct_count: header.field_5.field_5.field_4,
      max_value: header.field_5.field_5.field_5,
      min_value: header.field_5.field_5.field_6,
    },
  }
  const dictionary_page_header: DictionaryPageHeader = header.field_7 && {
    num_values: header.field_7.field_1
  }
  const data_page_header_v2: DataPageHeaderV2 = header.field_8 && {
    num_values: header.field_8.field_1,
    num_nulls: header.field_8.field_2,
    num_rows: header.field_8.field_3,
    encoding: header.field_8.field_4,
    definition_levels_byte_length: header.field_8.field_5,
    repetition_levels_byte_length: header.field_8.field_6,
    is_compressed: header.field_8.field_7 === undefined ? true : header.field_8.field_7, // default true
    statistics: header.field_8.field_8,
  }

  return {
    type,
    uncompressed_page_size,
    compressed_page_size,
    data_page_header,
    dictionary_page_header,
    data_page_header_v2,
  }
}
