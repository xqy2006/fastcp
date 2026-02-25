#pragma once

// ============================================================
// archive_builder.hpp -- Virtual Archive layout builder
//
// Assigns a virtual byte offset to every file and divides the
// resulting virtual stream into fixed-size archive chunks.
// Chunk boundaries may span file boundaries.
// ============================================================

#include "../common/platform.hpp"
#include "../common/hash.hpp"
#include "dir_scanner.hpp"
#include <vector>
#include <string>

// One contiguous slice of a single file that belongs to an archive chunk.
struct ChunkSpan {
    u32 file_idx;    // index into ArchiveBuilder::files()
    u64 file_offset; // byte offset within the source file
    u32 length;      // number of bytes
};

// One fixed-size chunk of the virtual archive stream.
struct VirtualChunk {
    u32 chunk_id;
    u64 archive_offset; // byte offset in virtual stream
    u32 raw_size;       // actual bytes (last chunk may be smaller)
    std::vector<ChunkSpan> spans; // file slices that make up this chunk
};

// File as it appears in the virtual archive.
struct VirtualFile {
    u32 file_id;
    u64 virtual_offset; // byte offset in virtual stream
    u64 file_size;
    u64 mtime_ns;
    hash::Hash128 xxh3_128;
    std::string rel_path;
    std::string abs_path;
};

// Default memory limit for pre_read_all(): 256 MiB.
static constexpr u64 PRE_READ_MAX_BYTES = 256ULL * 1024 * 1024;

class ArchiveBuilder {
public:
    // Build virtual layout from file entries.
    // chunk_size: fixed archive chunk size in bytes.
    void build(const std::vector<FileEntry>& entries, u32 chunk_size);

    const std::vector<VirtualFile>& files()  const { return files_;  }
    const std::vector<VirtualChunk>& chunks() const { return chunks_; }
    u64 total_size()  const { return total_size_;  }
    u32 chunk_size()  const { return chunk_size_;  }

    // Pre-read ALL file content into a single in-memory buffer and compute
    // per-file xxh3_128 hashes in the same pass (one open/read/close per file).
    //
    // Returns true if the buffer was successfully populated.  After this call:
    //   - files_[i].xxh3_128 is set for every file
    //   - read_chunk() serves data directly from the buffer (no disk I/O)
    //
    // Returns false (and leaves state unchanged) if total_size_ exceeds
    // max_bytes — in that case read_chunk() falls back to MmapReader.
    bool pre_read_all(u64 max_bytes = PRE_READ_MAX_BYTES);

    // Read and assemble raw data for one archive chunk.
    // If pre_read_all() succeeded, this is a fast in-memory memcpy.
    // Otherwise falls back to opening each source file individually.
    std::vector<u8> read_chunk(u32 chunk_id) const;

private:
    std::vector<VirtualFile>  files_;
    std::vector<VirtualChunk> chunks_;
    u64 total_size_{0};
    u32 chunk_size_{0};

    // In-memory buffer populated by pre_read_all(); empty if not called.
    std::vector<u8> pre_read_buf_;
};
