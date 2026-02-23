#pragma once

// ============================================================
// file_io.hpp -- Memory-mapped file I/O
// ============================================================

#include "platform.hpp"
#include <string>
#include <vector>
#include <stdexcept>
#include <cstdint>
#include <filesystem>

namespace fs = std::filesystem;

namespace file_io {

// ---- MmapReader: zero-copy read via mmap ----
class MmapReader {
public:
    explicit MmapReader(const std::string& path);
    ~MmapReader();

    MmapReader(const MmapReader&) = delete;
    MmapReader& operator=(const MmapReader&) = delete;

    const char* data() const { return data_; }
    u64 size() const { return size_; }

    // Get pointer to chunk at given offset, clamped to available bytes
    const char* chunk_ptr(u64 offset) const {
        if (offset >= size_) return nullptr;
        return data_ + offset;
    }

    u64 chunk_len(u64 offset, u64 max_len) const {
        if (offset >= size_) return 0;
        u64 remaining = size_ - offset;
        return remaining < max_len ? remaining : max_len;
    }

    void close();

private:
    const char* data_{nullptr};
    u64 size_{0};

#ifdef _WIN32
    HANDLE file_handle_{INVALID_HANDLE_VALUE};
    HANDLE map_handle_{nullptr};
#else
    int fd_{-1};
#endif
};

// ---- MmapWriter: zero-copy write via mmap ----
class MmapWriter {
public:
    MmapWriter() = default;
    ~MmapWriter();

    MmapWriter(const MmapWriter&) = delete;
    MmapWriter& operator=(const MmapWriter&) = delete;

    // Open/create file, preallocate to size, then mmap.
    // If resume=true, open existing file without truncating (for partial resume).
    void open(const std::string& path, u64 size, bool resume = false);

    // Write data at given offset
    void write_at(u64 offset, const void* data, size_t len);

    // Flush and close; optionally set modification time
    void close(u64 mtime_ns = 0);

    bool is_open() const { return data_ != nullptr; }
    u64 size() const { return size_; }

private:
    char* data_{nullptr};
    u64   size_{0};

#ifdef _WIN32
    HANDLE file_handle_{INVALID_HANDLE_VALUE};
    HANDLE map_handle_{nullptr};
    std::string path_;
#else
    int fd_{-1};
    std::string path_;
#endif
};

// ---- Utility functions ----

// Pre-allocate file space (may create sparse holes on Windows)
void preallocate(const std::string& path, u64 size);

// Set file modification time (nanoseconds since epoch)
void set_mtime(const std::string& path, u64 mtime_ns);

// Sanitize relative path to prevent directory traversal
// Throws if path is unsafe
fs::path proto_to_fspath(const fs::path& root_dir, const std::string& relative_path);

// Create parent directories if they don't exist
void ensure_parent_dirs(const std::string& path);

// Get file size in bytes; returns 0 if not found
u64 get_file_size(const std::string& path);

// Get file modification time as nanoseconds since epoch
u64 get_mtime_ns(const std::string& path);

// Read entire small file into memory
std::vector<u8> read_small_file(const std::string& path);

} // namespace file_io
