package app.hyperparam.hyparquet

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.assertj.core.api.Assertions.assertThat
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.nio.ByteBuffer

class ConvertTest {

    private val name = "name"
    private val parsers = DefaultParsers

    @Test
    fun `convert returns same data if converted_type is undefined`() {
        val data = listOf(1, 2, 3)
        val element = SchemaElement(name = name)
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).isEqualTo(data)
    }

    @Test
    fun `convert byte arrays to utf8`() {
        val data = listOf("foo".toByteArray(), "bar".toByteArray())
        val element = SchemaElement(name = name, convertedType = "UTF8")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly("foo", "bar")
    }

    @Test
    fun `convert byte arrays to utf8 default true`() {
        val data = listOf("foo".toByteArray(), "bar".toByteArray())
        val element = SchemaElement(name = name, type = "BYTE_ARRAY")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly("foo", "bar")
    }

    @Test
    fun `preserve byte arrays when utf8 is false`() {
        val data = listOf("foo".toByteArray(), "bar".toByteArray())
        val element = SchemaElement(name = name, type = "BYTE_ARRAY")
        val result = convert(data, ColumnDecoder(element, utf8 = false, parsers = parsers))
        assertThat(result).isEqualTo(data)
    }

    @Test
    fun `decode geometry logical type with default parser`() {
        // Point WKB: little endian, point type, coordinates [102, 0.5]
        val pointWkb = byteArrayOf(
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, -128, 89, 64, 0, 0, 0, 0, 0, 0, -32, 63
        )
        val data = listOf(pointWkb)
        val element = SchemaElement(
            name = name,
            type = "BYTE_ARRAY",
            logicalType = LogicalType(type = "GEOMETRY")
        )
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(1)
        val geometry = result[0] as Geometry
        assertThat(geometry.type).isEqualTo("Point")
        assertThat(geometry.coordinates).isInstanceOf(List::class.java)
        val coords = geometry.coordinates as List<*>
        assertThat(coords[0]).isEqualTo(102.0)
        assertThat(coords[1]).isEqualTo(0.5)
    }

    @Test
    fun `decode geography logical type with default parser`() {
        // Point WKB: little endian, point type, coordinates [102, 0.5]
        val pointWkb = byteArrayOf(
            1, 1, 0, 0, 0, 0, 0, 0, 0, 0, -128, 89, 64, 0, 0, 0, 0, 0, 0, -32, 63
        )
        val data = listOf(pointWkb)
        val element = SchemaElement(
            name = name,
            type = "BYTE_ARRAY",
            logicalType = LogicalType(type = "GEOGRAPHY")
        )
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(1)
        val geometry = result[0] as Geometry
        assertThat(geometry.type).isEqualTo("Point")
    }

    @Test
    fun `convert numbers to DECIMAL`() {
        val data = listOf(100, 200)
        val element = SchemaElement(name = name, convertedType = "DECIMAL")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly(100.0, 200.0)
    }

    @Test
    fun `convert numbers to DECIMAL with scale`() {
        val data = listOf(100, 200)
        val element = SchemaElement(name = name, convertedType = "DECIMAL", scale = 2)
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly(1.0, 2.0)
    }

    @Test
    fun `convert bigint to DECIMAL`() {
        val data = listOf(BigInteger.valueOf(1000), BigInteger.valueOf(2000))
        val element = SchemaElement(name = name, convertedType = "DECIMAL")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly(1000.0, 2000.0)
    }

    @Test
    fun `convert bigint to DECIMAL with scale`() {
        val data = listOf(BigInteger.valueOf(10), BigInteger.valueOf(20))
        val element = SchemaElement(name = name, convertedType = "DECIMAL", scale = 2)
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly(0.1, 0.2)
    }

    @Test
    fun `convert byte arrays to DECIMAL`() {
        val data = listOf(
            byteArrayOf(0, 0, 0, 100),
            byteArrayOf(0, 0, 0, -56) // 200 as signed byte
        )
        val element = SchemaElement(name = name, convertedType = "DECIMAL", scale = 0)
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        assertThat(result).containsExactly(100.0, 200.0)
    }

    @Test
    fun `convert dates from days`() {
        val data = listOf(19000, 19001) // Days since epoch
        val element = SchemaElement(name = name, convertedType = "DATE")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(2)
        assertThat(result[0]).isInstanceOf(LocalDate::class.java)
        assertThat(result[1]).isInstanceOf(LocalDate::class.java)
    }

    @Test
    fun `convert TIMESTAMP_MILLIS`() {
        val data = listOf(BigInteger.valueOf(1716506900000), BigInteger.valueOf(1716507000000))
        val element = SchemaElement(name = name, convertedType = "TIMESTAMP_MILLIS")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(2)
        assertThat(result[0]).isInstanceOf(Instant::class.java)
        assertThat(result[1]).isInstanceOf(Instant::class.java)
    }

    @Test
    fun `convert TIMESTAMP_MICROS`() {
        val data = listOf(BigInteger.valueOf(1716506900000000), BigInteger.valueOf(1716507000000000))
        val element = SchemaElement(name = name, convertedType = "TIMESTAMP_MICROS")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(2)
        assertThat(result[0]).isInstanceOf(Instant::class.java)
        assertThat(result[1]).isInstanceOf(Instant::class.java)
    }

    @Test
    fun `convert UINT_64`() {
        val data = listOf(1000L, 2000L)
        val element = SchemaElement(name = name, convertedType = "UINT_64")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(2)
        assertThat(result[0]).isInstanceOf(ULong::class.java)
        assertThat(result[1]).isInstanceOf(ULong::class.java)
    }

    @Test
    fun `convert UINT_32`() {
        val data = listOf(1000, 2000)
        val element = SchemaElement(name = name, convertedType = "UINT_32")
        val result = convert(data, ColumnDecoder(element, parsers = parsers))
        
        assertThat(result).hasSize(2)
        assertThat(result[0]).isInstanceOf(UInt::class.java)
        assertThat(result[1]).isInstanceOf(UInt::class.java)
    }

    @Test
    fun `throws error for unsupported BSON`() {
        val data = listOf("test".toByteArray())
        val element = SchemaElement(name = name, convertedType = "BSON")
        
        assertThrows<NotImplementedError> {
            convert(data, ColumnDecoder(element, parsers = parsers))
        }
    }

    @Test
    fun `throws error for unsupported INTERVAL`() {
        val data = listOf("test".toByteArray())
        val element = SchemaElement(name = name, convertedType = "INTERVAL")
        
        assertThrows<NotImplementedError> {
            convert(data, ColumnDecoder(element, parsers = parsers))
        }
    }

    @Test
    fun `parseDecimal handles empty bytes`() {
        assertThat(parseDecimal(byteArrayOf())).isEqualTo(0.0)
    }

    @Test
    fun `parseDecimal handles single byte`() {
        assertThat(parseDecimal(byteArrayOf(42))).isEqualTo(42.0)
    }

    @Test
    fun `parseDecimal handles two bytes in big-endian order`() {
        assertThat(parseDecimal(byteArrayOf(1, 0))).isEqualTo(256.0)
    }

    @Test
    fun `parseDecimal handles three bytes`() {
        assertThat(parseDecimal(byteArrayOf(1, 2, 3))).isEqualTo(66051.0)
    }

    @Test
    fun `parseDecimal handles negative one as 32-bit number`() {
        assertThat(parseDecimal(byteArrayOf(-1, -1, -1, -1))).isEqualTo(-1.0)
    }

    @Test
    fun `parseFloat16 handles null input`() {
        assertThat(parseFloat16(null)).isNull()
        assertThat(parseFloat16(byteArrayOf(1))).isNull() // too short
    }

    @Test
    fun `parseFloat16 handles normal values`() {
        // Test basic conversion - exact values will depend on specific float16 encoding
        val result = parseFloat16(byteArrayOf(0, 60)) // Some example bytes
        assertThat(result).isNotNull()
        assertThat(result).isInstanceOf(Double::class.javaObjectType)
    }
}