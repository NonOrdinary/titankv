# TitanKV Go to C++20 Storage Engine Migration Plan

This document maps out the translation of the Go-based LSM storage engine in `internal/engine` to Modern C++ (C++20).

## 1. Class Equivalents & File Architecture

| Go File / Struct | C++ Class / Struct | Header / Source | Key Notes / Modern C++ Features |
| :--- | :--- | :--- | :--- |
| `bloomfilter.go`<br>`BloomFilter` | `BloomFilter` | `src/bloomfilter.hpp`<br>`src/bloomfilter.cpp` | Uses `std::vector<uint8_t>` for memory. FNV-1a custom 32-bit hash function matching the specific sequence offset. |
| `internal_key.go`<br>`TypeDelete` / `TypePut` | `InternalKeyType` (enum) | `src/internal_key.hpp`<br>`src/internal_key.cpp` | Implements key encoding, decoding, and custom comparator. Converts Go's slice copies to `std::vector<uint8_t>`. |
| `skiplist.go`<br>`Node` / `SkipList` | `SkipList` / `Node` | `src/skiplist.hpp` | Uses `std::unique_ptr` or raw pointers cleanly destroyed in destructor. Uses `<random>` instead of `rand.Seed`. |
| `wal.go`<br>`WAL` | `WAL` | `src/wal.hpp`<br>`src/wal.cpp` | Uses `std::fstream` for files, and `std::filesystem::resize_file` for corruption truncation. |
| `memtable.go`<br>`MemTable` | `MemTable` | `src/memtable.hpp`<br>`src/memtable.cpp` | Thread-safe reads/writes using `std::shared_mutex` and locks (`std::shared_lock`/`std::unique_lock`). |
| `sstable_builder.go`<br>`SSTableBuilder` | `SSTableBuilder` | `src/sstable_builder.hpp`<br>`src/sstable_builder.cpp` | Writes binary structures. Scratch buffer reuse using `std::vector<uint8_t>`. |
| `sstable_reader.go`<br>`SSTableReader` | `SSTableReader` | `src/sstable_reader.hpp`<br>`src/sstable_reader.cpp` | Manages file access with `std::ifstream` and a `std::mutex` for thread-safe concurrent `Get` queries. |
| `sstable_iterator.go`<br>`SSTableIterator` | `SSTableIterator` | `src/sstable_iterator.hpp`<br>`src/sstable_iterator.cpp` | Streams records sequentially. |
| `manifest.go`<br>`Manifest` | `Manifest` | `src/manifest.hpp`<br>`src/manifest.cpp` | Parses and logs JSON records. Uses a dependency-free robust JSON reader/writer. |
| `db.go`<br>`DB` | `DB` | `src/db.hpp`<br>`src/db.cpp` | Coordinates active/immutable memtables, active WAL, SSTables list, and background tasks. |
| (N/A) | `Channel<T>` | `src/channel.hpp` | Implements channel-like queue for coordinating asynchronous background flushing. |

## 2. Memory Management Strategy
1. **RAII (Resource Acquisition Is Initialization)**: All file resources (`std::fstream`, `std::ifstream`, `std::ofstream`) will be owned by their respective classes and automatically closed when they go out of scope.
2. **Dynamic Objects**:
   - `MemTable`s and `WAL` instances will be managed via `std::unique_ptr` where ownership is exclusive.
   - `SSTableReader` objects will be managed using `std::shared_ptr<SSTableReader>`. This ensures reader objects are kept alive as long as any reader thread holds them, even if a concurrent compaction merges and replaces them in the active tables array.
3. **Containers**: We avoid manual allocations and raw pointer arithmetic for key/value storage. Instead, we use `std::vector<uint8_t>` for byte slices, which handles dynamic allocation and cleanup.

## 3. Strict Binary Compatibility
- **Bloom Filter**: Crucially, the 32-bit FNV-1a hash must match:
  - FNV Offset Basis: `2166136261U`
  - FNV Prime: `16777619U`
  - Combinator sequence: `combinedHash = hash + i * 0x9e3779b9U`
- **Byte Order**: We explicitly use big-endian shifting for internal keys (sequence numbers) and little-endian shifting for file lengths/offsets in SSTable blocks, index, and footer. This guarantees compatibility on both big-endian and little-endian CPUs.
- **SSTable/WAL Checksums**: We will implement standard CRC32 IEEE computation matching Go's `crc32.ChecksumIEEE`.

## 4. Build and Verification Plan
- **Build System**: CMake (minimum version 3.15, standard C++20).
- **Unit Testing**: Google Test will be fetched automatically using CMake's `FetchContent`.
- **Test Cases**:
  - `TestTitanKV_TimeTravel`: Verifies multiversion retrieval using sequence numbers.
  - `TestTitanKV_CompactionPurge`: Verifies that repeated updates trigger flushes/compactions and that dead keys are correctly purged without losing active data.
