import {assembleLists} from './assemble.js'
import {
  ColumnData,
  ColumnDecoder, DataPageHeader, DataPageHeaderV2,
  DataReader,
  DecodedArray, DictionaryPageHeader,
  FieldRepetitionType,
  PageHeader,
  PageType,
  RowGroupSelect, ThriftObject
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
export function readColumn(
    reader: DataReader,
    rowGroupSelect: RowGroupSelect,
    columnDecoder: ColumnDecoder,
    onPage: (chunk: ColumnData) => void =  _ => {},
): DecodedArray[] {
  const { groupStart, selectStart, selectEnd} = rowGroupSelect
  const { columnName, schemaPath } = columnDecoder
  const isFlat = isFlatColumn(schemaPath!)
  const chunks: DecodedArray[] = []
  let dictionary: DecodedArray | undefined = undefined
  let lastChunk: DecodedArray | undefined = undefined
  let rowCount = 0

  const emitLastChunk = onPage !== undefined ? (() => {
    if (lastChunk !== undefined) {
      onPage({
        columnName: columnName!,
        columnData: lastChunk,
        rowStart: groupStart + rowCount - lastChunk.length,
        rowEnd: groupStart + rowCount,
      })
    }
  }) : undefined

  while (isFlat ? rowCount < selectEnd : reader.offset < reader.view.byteLength - 1) {
    if (reader.offset >= reader.view.byteLength - 1) break // end of reader

    // read page header
    const header = parquetHeader(reader)
    if (header.type === PageType.DICTIONARY_PAGE) {
      // assert(!dictionary)
      dictionary = readPage(reader, header, columnDecoder, dictionary, undefined, 0)
      dictionary = convert(dictionary, columnDecoder)
    } else {
      const lastChunkLength = lastChunk !== undefined ? lastChunk.length : 0
      const values = readPage(reader, header, columnDecoder, dictionary, lastChunk, selectStart - rowCount)
      if (lastChunk === values) {
        // continued from previous page
        rowCount += values.length - lastChunkLength
      } else {
        if (emitLastChunk !== undefined) emitLastChunk()
        chunks.push(values)
        rowCount += values.length
        lastChunk = values
      }
    }
  }
  if (emitLastChunk !== undefined) emitLastChunk()
  // assert(rowCount >= selectEnd)
  if (rowCount > selectEnd && lastChunk !== undefined) {
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
    if (daph === undefined) throw new Error('parquet data page header is undefined')

    // skip unnecessary non-nested pages
    if (pageStart > daph.num_values && isFlatColumn(schemaPath!)) {
      return new Array(daph.num_values) // TODO: don't allocate array
    }

    const page = decompressPage(compressedBytes, Number(header.uncompressed_page_size), codec!, compressors)
    const { definitionLevels, repetitionLevels, dataPage } = readDataPage(page, daph, columnDecoder)
    // assert(!daph.statistics?.null_count || daph.statistics.null_count === BigInt(daph.num_values - dataPage.length))

    // convert types, dereference dictionary, and assemble lists
    let values = convertWithDictionary(dataPage, dictionary, daph.encoding, columnDecoder)
    if (repetitionLevels.length > 0 || (definitionLevels !== undefined && definitionLevels.length > 0)) {
      const output = Array.isArray(previousChunk) ? previousChunk : []
      return assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath!)
    } else {
      // wrap nested flat data by depth
      for (let i = 2; i < schemaPath!.length; i++) {
        if (schemaPath![i].element.repetition_type !== FieldRepetitionType.REQUIRED) {
          values = Array.from(values, e => [e])
        }
      }
      return values
    }
  } else if (header.type === PageType.DATA_PAGE_V2) {
    const daph2 = header.data_page_header_v2
    if (daph2 === undefined) throw new Error('parquet data page header v2 is undefined')

    // skip unnecessary pages
    if (pageStart > daph2.num_rows) {
      return new Array(daph2.num_values) // TODO: don't allocate array
    }

    const { definitionLevels, repetitionLevels, dataPage } =
      readDataPageV2(compressedBytes, header, columnDecoder)

    // convert types, dereference dictionary, and assemble lists
    const values = convertWithDictionary(dataPage, dictionary, daph2.encoding, columnDecoder)
    const output = Array.isArray(previousChunk) ? previousChunk : []
    return assembleLists(output, definitionLevels, repetitionLevels, values, schemaPath!)
  } else if (header.type === PageType.DICTIONARY_PAGE) {
    const diph = header.dictionary_page_header
    if (diph === undefined) throw new Error('parquet dictionary page header is undefined')

    const page = decompressPage(
      compressedBytes, Number(header.uncompressed_page_size), codec!, compressors
    )

    const reader = { view: new DataView(page.buffer, page.byteOffset, page.byteLength), offset: 0 }
    return readPlain(reader, type!, diph.num_values, element.type_length)
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
  const type: PageType = header[1] as number
  const uncompressed_page_size = header[2] as number
  const compressed_page_size = header[3] as number
  const header5 = header[5] as ThriftObject | undefined
  let data_page_header: DataPageHeader | undefined = undefined
  if (header5 !== undefined) {
    data_page_header = {
      num_values: header5[1] as number,
      encoding: header5[2] as number,
    }
  }
  const header7 = header[7] as ThriftObject | undefined
  let dictionary_page_header: DictionaryPageHeader | undefined = undefined
  if (header7 !== undefined) {
    dictionary_page_header = {
      num_values: header7[1] as number,
    }
  }
  const header8 = header[8] as ThriftObject | undefined
  let data_page_header_v2: DataPageHeaderV2 | undefined = undefined
  if (header8 !== undefined) {
    const header87 = header8[7] as boolean | undefined
    data_page_header_v2 = {
      num_values: header8[1] as number,
      num_nulls: header8[2] as number,
      num_rows: header8[3] as number,
      encoding: header8[4] as number,
      definition_levels_byte_length: header8[5] as number,
      repetition_levels_byte_length: header8[6] as number,
      is_compressed: header87 === undefined ? true : header87, // default true
    }
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
