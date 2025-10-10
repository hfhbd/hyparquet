import { DEFAULT_PARSERS } from './convert.js'
import { convertMetadata } from './metadata.js'
import { deserializeTCompactProtocol } from './thrift.js'
import {
  ColumnIndex,
  DataReader,
  OffsetIndex,
  PageLocation,
  ParquetParsers,
  SchemaElement
} from "./types.js"

/**
 * @param {DataReader} reader
 * @param {SchemaElement} schema
 * @param {ParquetParsers | undefined} parsers
 * @returns {ColumnIndex}
 */
export function readColumnIndex(reader: DataReader, schema: SchemaElement, parsers: ParquetParsers | undefined = undefined): ColumnIndex {
  parsers = { ...DEFAULT_PARSERS, ...parsers }

  const thrift = deserializeTCompactProtocol(reader)
  return {
    null_pages: thrift.field_1,
    min_values: thrift.field_2.map((m: any) => convertMetadata(m, schema, parsers)),
    max_values: thrift.field_3.map((m: any) => convertMetadata(m, schema, parsers)),
    boundary_order: thrift.field_4,
    null_counts: thrift.field_5,
    repetition_level_histograms: thrift.field_6,
    definition_level_histograms: thrift.field_7,
  }
}

export function readOffsetIndex(reader: DataReader): OffsetIndex {
  const thrift = deserializeTCompactProtocol(reader)
  return {
    page_locations: thrift.field_1.map(pageLocation),
    unencoded_byte_array_data_bytes: thrift.field_2,
  }
}

function pageLocation(loc: any): PageLocation {
  return {
    offset: loc.field_1,
    compressed_page_size: loc.field_2,
    first_row_index: loc.field_3,
  }
}
