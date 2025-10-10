package app.hyperparam.hyparquet

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.assertj.core.api.Assertions.assertThat
import java.nio.ByteBuffer
import java.nio.ByteOrder

class WkbTest {

    private fun makeReader(buffer: ByteArray): DataReader {
        return DataReader(ByteBuffer.wrap(buffer), 0)
    }

    @Test
    fun `decode little-endian Point`() {
        val buffer = byteArrayOf(
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, -128, 89, 64, 0, 0, 0, 0, 0, 0, -32, 63
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("Point")
        val coords = result.coordinates as List<*>
        assertThat(coords[0]).isEqualTo(102.0)
        assertThat(coords[1]).isEqualTo(0.5)
    }

    @Test
    fun `decode big-endian LineString`() {
        val buffer = byteArrayOf(
            0, 0, 0, 0, 2, 0, 0, 0, 2, 63, -8, 0, 0, 0, 0, 0, 0, -64, 12, 0,
            0, 0, 0, 0, 0, 64, 17, 0, 0, 0, 0, 0, 0, 64, 23, 0, 0, 0, 0, 0, 0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("LineString")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(2)
        
        val point1 = coords[0] as List<*>
        assertThat(point1[0]).isEqualTo(1.5)
        assertThat(point1[1]).isEqualTo(-3.5)
        
        val point2 = coords[1] as List<*>
        assertThat(point2[0]).isEqualTo(4.25)
        assertThat(point2[1]).isEqualTo(5.75)
    }

    @Test
    fun `decode little-endian Polygon`() {
        val buffer = byteArrayOf(
            1, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16,
            63, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("Polygon")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(1)
        
        val ring = coords[0] as List<*>
        assertThat(ring).hasSize(4)
        
        val point1 = ring[0] as List<*>
        assertThat(point1[0]).isEqualTo(0.0)
        assertThat(point1[1]).isEqualTo(0.0)
        
        val point2 = ring[1] as List<*>
        assertThat(point2[0]).isEqualTo(1.0)
        assertThat(point2[1]).isEqualTo(0.0)
    }

    @Test
    fun `decode little-endian MultiLineString`() {
        val buffer = byteArrayOf(
            1, 5, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0,
            0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0, -16, 63, 0, 0, 0, 0, 0, 0,
            0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 1, 2, 0, 0, 0, 2, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 0,
            0, 16, 64, 0, 0, 0, 0, 0, 0, 16, 64
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("MultiLineString")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(2)
    }

    @Test
    fun `decode mixed-endian MultiPoint`() {
        val buffer = byteArrayOf(
            1, 4, 0, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 64, 0, 0, 0, 0, 0, 0, 8, 64, 0, 0, 0, 0, 1, -65, -16, 0, 0, 0,
            0, 0, 0, 63, -32, 0, 0, 0, 0, 0, 0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("MultiPoint")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(2)
        
        val point1 = coords[0] as List<*>
        assertThat(point1[0]).isEqualTo(2.0)
        assertThat(point1[1]).isEqualTo(3.0)
        
        val point2 = coords[1] as List<*>
        assertThat(point2[0]).isEqualTo(-1.0)
        assertThat(point2[1]).isEqualTo(0.5)
    }

    @Test
    fun `decode nested MultiPolygon`() {
        val buffer = byteArrayOf(
            1, 6, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0,
            0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0,
            0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("MultiPolygon")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(1)
    }

    @Test
    fun `decode GeometryCollection`() {
        val buffer = byteArrayOf(
            1, 7, 0, 0, 0, 2, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, -16,
            63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 2, 0, 0, 0, 2, 64, 8, 0,
            0, 0, 0, 0, 0, 64, 16, 0, 0, 0, 0, 0, 0, 64, 20, 0, 0, 0, 0, 0, 0,
            64, 24, 0, 0, 0, 0, 0, 0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("GeometryCollection")
        assertThat(result.geometries).isNotNull()
        assertThat(result.geometries).hasSize(2)
        
        val point = result.geometries!![0]
        assertThat(point.type).isEqualTo("Point")
        
        val lineString = result.geometries!![1]
        assertThat(lineString.type).isEqualTo("LineString")
    }

    @Test
    fun `throw on unsupported geometry type`() {
        val buffer = byteArrayOf(1, 99, 0, 0, 0)

        assertThrows<IllegalArgumentException> {
            wkbToGeojson(makeReader(buffer))
        }.also { exception ->
            assertThat(exception.message).contains("Unsupported geometry type: 99")
        }
    }

    @Test
    fun `decode ISO WKB Point with Z and M flags`() {
        val buffer = byteArrayOf(
            1,
            -71, 11, 0, 0, // Type with Z and M flags (3001)
            0, 0, 0, 0, 0, 0, -16, 63, // X: 1.0
            0, 0, 0, 0, 0, 0, 0, 64,  // Y: 2.0
            0, 0, 0, 0, 0, 0, 8, 64,  // Z: 3.0
            0, 0, 0, 0, 0, 0, 16, 64  // M: 4.0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("Point")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(4)
        assertThat(coords[0]).isEqualTo(1.0)
        assertThat(coords[1]).isEqualTo(2.0)
        assertThat(coords[2]).isEqualTo(3.0)
        assertThat(coords[3]).isEqualTo(4.0)
    }

    @Test
    fun `decode point with M-only dimensional offset`() {
        val buffer = byteArrayOf(
            1, -47, 7, 0, 0, // Type with M flag only
            0, 0, 0, 0, 0, 0, 34, 64, // X: 9.0
            0, 0, 0, 0, 0, 0, 36, 64, // Y: 10.0
            0, 0, 0, 0, 0, 0, 38, 64  // M: 11.0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("Point")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(3)
        assertThat(coords[0]).isEqualTo(9.0)
        assertThat(coords[1]).isEqualTo(10.0)
        assertThat(coords[2]).isEqualTo(11.0)
    }

    @Test
    fun `decode point with Z-only dimensional offset`() {
        val buffer = byteArrayOf(
            1, -23, 3, 0, 0, // Type with Z flag only
            0, 0, 0, 0, 0, 0, 40, 64, // X: 12.0
            0, 0, 0, 0, 0, 0, 42, 64, // Y: 13.0
            0, 0, 0, 0, 0, 0, 44, 64  // Z: 14.0
        )

        val result = wkbToGeojson(makeReader(buffer))
        assertThat(result.type).isEqualTo("Point")
        val coords = result.coordinates as List<*>
        assertThat(coords).hasSize(3)
        assertThat(coords[0]).isEqualTo(12.0)
        assertThat(coords[1]).isEqualTo(13.0)
        assertThat(coords[2]).isEqualTo(14.0)
    }
}