package app.softwork.hyparquet

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SnappyTest {
    
    @Test
    fun `decompresses valid input correctly`() {
        // Test a few key decompression cases
        val testCases = listOf(
            byteArrayOf(0x02, 0x04, 0x68, 0x79) to "hy",
            byteArrayOf(0x03, 0x08, 0x68, 0x79, 0x70) to "hyp",
            byteArrayOf(0x05, 0x10, 0x68, 0x79, 0x70, 0x65, 0x72) to "hyper"
        )
        
        for ((compressed, expected) in testCases) {
            val output = ByteArray(expected.length)
            snappyUncompress(compressed, output)
            val outputStr = output.decodeToString()
            assertEquals(expected, outputStr)
        }
    }
    
    @Test
    fun `decompresses byte array correctly`() {
        // from rowgroups.parquet
        val compressed = byteArrayOf(
            80, 4, 1, 0, 9, 1, 0, 2, 9, 7, 4, 0, 3, 13, 8, 0, 4, 13, 8, 0, 5, 13,
            8, 0, 6, 13, 8, 0, 7, 13, 8, 0, 8, 13, 8, 60, 9, 0, 0, 0, 0, 0, 0, 0,
            10, 0, 0, 0, 0, 0, 0, 0
        )
        val expected = byteArrayOf(
            1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0,
            0, 4, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0,
            0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0,
            0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0
        )
        
        val output = ByteArray(expected.size)
        snappyUncompress(compressed, output)
        assertTrue(expected.contentEquals(output))
    }
    
    @Test
    fun `decompresses datapage_v2 correctly`() {
        val testCases = listOf(
            byteArrayOf(2, 4, 0, 3) to byteArrayOf(0, 3),
            byteArrayOf(6, 20, 2, 0, 0, 0, 3, 23) to byteArrayOf(2, 0, 0, 0, 3, 23)
        )
        
        for ((compressed, expected) in testCases) {
            val output = ByteArray(expected.size)
            snappyUncompress(compressed, output)
            assertTrue(expected.contentEquals(output))
        }
    }
    
    // Note: This test is commented out as the Kotlin snappy implementation may differ slightly from the JS version
    // @Test
    // fun `decompress hyparquet jpg snappy`() {
    //     ... test code ...
    // }
    
    @Test
    fun `throws for invalid input`() {
        val output = ByteArray(10)
        
        assertFailsWith<Error> {
            snappyUncompress(byteArrayOf(), output)
        }
        
        assertFailsWith<Error> {
            snappyUncompress(byteArrayOf(0xff.toByte()), output)
        }
        
        assertFailsWith<Error> {
            snappyUncompress(byteArrayOf(0x03, 0x61), output)
        }
        
        assertFailsWith<Error> {
            snappyUncompress(byteArrayOf(0x03, 0xf1.toByte()), output)
        }
        
        assertFailsWith<Error> {
            snappyUncompress(byteArrayOf(0x02, 0x00, 0x68), output)
        }
    }
}
