# Hyparquet Kotlin/JVM

A Kotlin/JVM port of the [hyparquet](https://github.com/hyparam/hyparquet) JavaScript library for reading Apache Parquet files.

## Features

- **Pure Kotlin/JVM**: No native dependencies, runs on any JVM
- **Type Safe**: Leverages Kotlin's strong type system
- **Async Support**: Built with Kotlin coroutines for non-blocking I/O
- **Multiple Input Sources**: Read from byte arrays, URLs, or custom AsyncBuffer implementations
- **Flexible Output**: Get data as objects (maps) or arrays
- **Custom Parsers**: Support for custom type parsing

## Quick Start

### Gradle

Add to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.hfhbd:hyparquet-kotlin:1.0.0")
}
```

### Basic Usage

```kotlin
import io.github.hfhbd.hyparquet.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // Read from a byte array
    val parquetData: ByteArray = loadParquetFile() // Your parquet data
    val buffer = Hyparquet.fromByteArray(parquetData)
    
    // Read all data as objects (maps)
    val rows = Hyparquet.readObjects(buffer)
    println("Read ${rows.size} rows")
    rows.forEach { row ->
        println(row)
    }
    
    // Read specific columns
    val specificColumns = Hyparquet.readObjects(
        buffer, 
        columns = listOf("name", "age", "email")
    )
    
    // Read a range of rows
    val firstHundredRows = Hyparquet.readObjects(
        buffer,
        rowStart = 0,
        rowEnd = 100
    )
    
    // Read as arrays instead of objects
    val arrayData = Hyparquet.readArrays(buffer)
}
```

### Reading from URL

```kotlin
import io.github.hfhbd.hyparquet.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    // Read from a remote parquet file
    val buffer = Hyparquet.fromUrl("https://example.com/data.parquet")
    val rows = Hyparquet.readObjects(buffer)
    println("Downloaded and read ${rows.size} rows")
}
```

### Custom Type Parsers

```kotlin
import io.github.hfhbd.hyparquet.*
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.runBlocking

class CustomParsers : ParquetParsers {
    override fun timestampFromMilliseconds(millis: Long) = Instant.ofEpochMilli(millis)
    override fun timestampFromMicroseconds(micros: Long) = Instant.ofEpochMilli(micros / 1000)
    override fun timestampFromNanoseconds(nanos: Long) = Instant.ofEpochMilli(nanos / 1_000_000)
    override fun dateFromDays(days: Int) = LocalDate.ofEpochDay(days.toLong())
    override fun stringFromBytes(bytes: ByteArray) = String(bytes, Charsets.UTF_8)
}

fun main() = runBlocking {
    val buffer = Hyparquet.fromByteArray(parquetData)
    val rows = Hyparquet.readObjects(buffer, parsers = CustomParsers())
    // Now timestamps will be Instant objects and dates will be LocalDate objects
}
```

### Low-Level API

For more control, you can use the lower-level APIs:

```kotlin
import io.github.hfhbd.hyparquet.*
import kotlinx.coroutines.runBlocking

fun main() = runBlocking {
    val buffer = Hyparquet.fromByteArray(parquetData)
    
    // Read metadata only
    val metadata = Hyparquet.readMetadata(buffer)
    println("Schema: ${metadata.schema}")
    println("Number of rows: ${metadata.numRows}")
    
    // Custom reading with callbacks
    parquetRead(ParquetReadOptions(
        file = buffer,
        columns = listOf("important_column"),
        rowStart = 0,
        rowEnd = 1000,
        onChunk = { chunk ->
            println("Received chunk for ${chunk.columnName}: ${chunk.columnData.size} values")
        },
        onComplete = { rows ->
            println("Reading complete: ${rows.size} rows")
        }
    ))
}
```

## Architecture

The library is structured into several key components:

- **Types.kt**: Core data types and schema definitions
- **Utils.kt**: Utility functions for data manipulation and I/O
- **Convert.kt**: Type conversion and parsing logic
- **Metadata.kt**: Parquet metadata reading and parsing
- **Read.kt**: Main reading API with async support
- **Hyparquet.kt**: High-level convenience API

## Supported Features

- ✅ Basic parquet file reading
- ✅ Type conversion (primitives, timestamps, decimals)
- ✅ Async/await support with coroutines
- ✅ Custom type parsers
- ✅ Column selection
- ✅ Row range selection
- ✅ Multiple input sources (byte arrays, URLs)
- ✅ Object and array output formats

## Not Yet Implemented

- ❌ Compression support (Snappy, GZIP, etc.)
- ❌ Complex nested types (lists, maps)
- ❌ Query/filtering support
- ❌ Writing parquet files
- ❌ Schema evolution

## Contributing

This is a port of the JavaScript hyparquet library. Contributions are welcome, especially for:

1. Implementing compression support
2. Adding query/filtering capabilities
3. Performance optimizations
4. Additional input/output formats

## License

MIT License - see LICENSE file for details.

## Credits

Based on the original [hyparquet](https://github.com/hyparam/hyparquet) JavaScript library by Hyperparam.