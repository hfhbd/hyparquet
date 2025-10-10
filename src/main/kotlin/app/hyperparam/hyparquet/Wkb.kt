package app.hyperparam.hyparquet

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * WKB flags extracted from binary data
 */
data class WkbFlags(
    val littleEndian: Boolean,
    val type: Int,
    val dim: Int,
    val count: Int
)

/**
 * WKB (Well-Known Binary) decoder for geometry objects.
 */
fun wkbToGeojson(reader: DataReader): Geometry {
    val flags = getFlags(reader)

    return when (flags.type) {
        1 -> { // Point
            Geometry(type = "Point", coordinates = readPosition(reader, flags))
        }
        2 -> { // LineString
            Geometry(type = "LineString", coordinates = readLine(reader, flags))
        }
        3 -> { // Polygon
            Geometry(type = "Polygon", coordinates = readPolygon(reader, flags))
        }
        4 -> { // MultiPoint
            val points = mutableListOf<List<Double>>()
            repeat(flags.count) {
                points.add(readPosition(reader, getFlags(reader)))
            }
            Geometry(type = "MultiPoint", coordinates = points)
        }
        5 -> { // MultiLineString
            val lines = mutableListOf<List<List<Double>>>()
            repeat(flags.count) {
                lines.add(readLine(reader, getFlags(reader)))
            }
            Geometry(type = "MultiLineString", coordinates = lines)
        }
        6 -> { // MultiPolygon
            val polygons = mutableListOf<List<List<List<Double>>>>()
            repeat(flags.count) {
                polygons.add(readPolygon(reader, getFlags(reader)))
            }
            Geometry(type = "MultiPolygon", coordinates = polygons)
        }
        7 -> { // GeometryCollection
            val geometries = mutableListOf<Geometry>()
            repeat(flags.count) {
                geometries.add(wkbToGeojson(reader))
            }
            Geometry(type = "GeometryCollection", coordinates = null, geometries = geometries)
        }
        else -> {
            throw IllegalArgumentException("Unsupported geometry type: ${flags.type}")
        }
    }
}

/**
 * Extract ISO WKB flags and base geometry type.
 */
private fun getFlags(reader: DataReader): WkbFlags {
    val view = reader.view
    val littleEndian = view.get(reader.offset++).toInt() == 1
    
    // Set byte order based on detected endianness
    view.order(if (littleEndian) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN)
    
    val rawType = view.getInt(reader.offset)
    reader.offset += 4

    val type = rawType % 1000
    val flags = rawType / 1000

    var count = 0
    if (type > 1 && type <= 7) {
        count = view.getInt(reader.offset)
        reader.offset += 4
    }

    // XY, XYZ, XYM, XYZM
    var dim = 2
    if (flags > 0) dim++
    if (flags == 3) dim++

    return WkbFlags(littleEndian, type, dim, count)
}

/**
 * Read a single position (point coordinates)
 */
private fun readPosition(reader: DataReader, flags: WkbFlags): List<Double> {
    val points = mutableListOf<Double>()
    repeat(flags.dim) {
        val coord = reader.view.getDouble(reader.offset)
        reader.offset += 8
        points.add(coord)
    }
    return points
}

/**
 * Read a line (array of positions)
 */
private fun readLine(reader: DataReader, flags: WkbFlags): List<List<Double>> {
    val points = mutableListOf<List<Double>>()
    repeat(flags.count) {
        points.add(readPosition(reader, flags))
    }
    return points
}

/**
 * Read a polygon (array of rings, each ring is an array of positions)
 */
private fun readPolygon(reader: DataReader, flags: WkbFlags): List<List<List<Double>>> {
    val view = reader.view
    val rings = mutableListOf<List<List<Double>>>()
    repeat(flags.count) {
        val count = view.getInt(reader.offset)
        reader.offset += 4
        rings.add(readLine(reader, flags.copy(count = count)))
    }
    return rings
}